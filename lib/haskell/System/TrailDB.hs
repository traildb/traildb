{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

-- ¦ Haskell bindings to trailDB library.
--
-- The bindings are multithread-safe (a thread is blocked until another thread
-- has finished operation) and asynchronous exception-safe. Garbage collector
-- finalizers are used to clean up anything that was not manually closed.
--

module System.TrailDB
  ( 
  -- * Constructing new TrailDBs
    newTrailDBCons
  , closeTrailDBCons
  , addCookie
  , appendTdbToTdbCons
  , finalizeTrailDBCons
  -- * Opening existing TrailDBs
  , openTrailDB
  , closeTrailDB
  , dontneedTrailDB
  , decodeTrailDB
  -- ** Features
  , field
  , value
  -- ** Basic querying
  , getNumCookies
  , getNumEvents
  , getNumFields
  , getMinTimestamp
  , getMaxTimestamp
  -- ** Cookie handling
  , getCookie
  , getCookieID
  -- ** Fields
  , getFieldName
  , getFieldID
  , getValue
  , getItem
  -- * Data types
  , Cookie
  , FieldName
  , FieldNameLike(..)
  , Feature()
  , TdbCons()
  , Tdb()
  -- ** Time
  , UnixTime
  , getUnixTime
  -- * Exceptions
  , TrailDBException(..)
  -- * Debugging
  , makeRandomTrailDB )
  where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as BL
import Data.Bits
import Data.Data
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock.POSIX
import Data.Word
import Foreign.C.String
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.Storable
import GHC.Generics

data TdbConsRaw
data TdbRaw

foreign import ccall unsafe tdb_cons_new
  :: Ptr CChar
  -> Ptr CChar
  -> Word32
  -> IO (Ptr TdbConsRaw)
foreign import ccall unsafe tdb_cons_free
  :: Ptr TdbConsRaw
  -> IO ()
foreign import ccall unsafe tdb_cons_add
  :: Ptr TdbConsRaw
  -> Ptr Word8
  -> Word32
  -> Ptr CChar
  -> IO CInt
foreign import ccall safe tdb_cons_finalize
  :: Ptr TdbConsRaw
  -> Word64
  -> IO CInt
foreign import ccall safe tdb_cons_append
  :: Ptr TdbConsRaw
  -> Ptr TdbRaw
  -> IO CInt
foreign import ccall unsafe tdb_dontneed
  :: Ptr TdbRaw
  -> IO ()
foreign import ccall safe tdb_open
  :: Ptr CChar
  -> IO (Ptr TdbRaw)
foreign import ccall safe tdb_close
  :: Ptr TdbRaw
  -> IO ()
foreign import ccall unsafe tdb_decode_trail
  :: Ptr TdbRaw
  -> Word64
  -> Ptr Word32
  -> Word32
  -> CInt
  -> IO Word32
foreign import ccall unsafe tdb_get_cookie_id
  :: Ptr TdbRaw
  -> Ptr Word8
  -> IO Word64
foreign import ccall unsafe tdb_get_cookie
  :: Ptr TdbRaw
  -> Word64
  -> IO (Ptr Word8)
foreign import ccall unsafe tdb_num_cookies
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_num_events
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_num_fields
  :: Ptr TdbRaw -> IO Word32
foreign import ccall unsafe tdb_min_timestamp
  :: Ptr TdbRaw -> IO Word32
foreign import ccall unsafe tdb_max_timestamp
  :: Ptr TdbRaw -> IO Word32
foreign import ccall unsafe tdb_get_field_name
  :: Ptr TdbRaw
  -> Word8
  -> IO (Ptr CChar)
foreign import ccall unsafe tdb_get_field
  :: Ptr TdbRaw
  -> Ptr CChar
  -> IO CInt
foreign import ccall unsafe tdb_get_item_value
   :: Ptr TdbRaw
   -> Word32
   -> IO (Ptr CChar)
foreign import ccall unsafe tdb_get_item
   :: Ptr TdbRaw
   -> Word8
   -> Ptr CChar
   -> IO Word32

-- | Cookies should be 16 bytes in size.
type Cookie = B.ByteString
type FieldName = B.ByteString
type UnixTime = Word32

type CookieID = Word64

type FieldID = Word8
type Value = Word32

-- | Exceptions that may happen with trailDBs.
--
-- Programming errors use `error` instead of throwing one of these exceptions.
data TrailDBException
  = CannotOpenTrailDBCons   -- ^ Failed to open `TdbCons`.
  | CannotOpenTrailDB       -- ^ Failed to open `Tdb`.
  | NoSuchCookieID          -- ^ A `CookieID` was used that doesn't exist in `Tdb`.
  | NoSuchCookie            -- ^ A `Cookie` was used that doesn't exist in `Tdb`.
  | NoSuchFieldID           -- ^ A `FieldID` was used that doesn't exist in `Tdb`.
  | NoSuchField             -- ^ A `Field` was used that doesn't exist in `Tdb`.
  | NoSuchValue             -- ^ A `Feature` was used that doesn't contain a valid value.
  deriving ( Eq, Ord, Show, Read, Typeable, Data, Generic, Enum )

instance Exception TrailDBException

newtype Feature = Feature Word32
  deriving ( Eq, Ord, Show, Read, Typeable, Data, Generic, Storable )

field :: Feature -> FieldID
field (Feature w) = fromIntegral $ w .&. 0x000000ff
{-# INLINE field #-}

value :: Feature -> Value
value (Feature w) = w `shiftR` 8
{-# INLINE value #-}

getUnixTime :: MonadIO m => m Word32
getUnixTime = liftIO $ do
  now <- getPOSIXTime
  let t = floor now
  return t
{-# LANGUAGE getUnixTime #-}

-- | Class of things that can be used as a field name.
--
-- The strict bytestring is the native type. Other types are converted,
-- encoding with UTF-8.
class FieldNameLike a where
  encodeToFieldName :: a -> B.ByteString
 
instance FieldNameLike String where
  encodeToFieldName = T.encodeUtf8 . T.pack

instance FieldNameLike B.ByteString where
  encodeToFieldName = id
  {-# INLINE encodeToFieldName #-}
 
instance FieldNameLike BL.ByteString where
  encodeToFieldName = BL.toStrict

newtype TdbCons = TdbCons (MVar (Maybe (Ptr TdbConsRaw)))
  deriving ( Typeable, Generic )

data TdbState = TdbState
  { tdbPtr :: {-# UNPACK #-} !(Ptr TdbRaw)
  , decodeBuffer :: {-# UNPACK #-} !(ForeignPtr Word32)
  , decodeBufferSize :: {-# UNPACK #-} !Word32 }

newtype Tdb = Tdb (MVar (Maybe TdbState))
  deriving ( Typeable, Generic )

-- ¦ Create a new trailDB. Returns a trailDB construction handle.
--
-- Close it with `closeTrailDBCons`. Garbage collector will close
-- it eventually if you didn't do it yourself.
--
-- @tdb_cons_new@.
newTrailDBCons :: (FieldNameLike a, MonadIO m)
               => FilePath
               -> [a]
               -> m TdbCons
newTrailDBCons filepath fields' = liftIO $ mask_ $
  withCString filepath $ \root ->
    allocaBytes fields_length_with_nulls $ \ofield_names -> do
      layoutNames fields ofield_names
      tdb_cons <- tdb_cons_new root ofield_names (fromIntegral $ length fields)

      when (tdb_cons == nullPtr) $
        throwIO CannotOpenTrailDBCons

      -- MVar will protect the handle from being used in multiple threads
      -- simultaneously.
      mvar <- newMVar (Just tdb_cons)

      -- Make garbage collector close it if it wasn't already.
      void $ mkWeakMVar mvar $ modifyMVar_ mvar $ \case
        Nothing -> return Nothing
        Just ptr -> tdb_cons_free ptr >> return Nothing

      return $ TdbCons mvar
 where
  fields_length_with_nulls =
    sum (fmap ((+1) . B.length) fields)
  fields = fmap encodeToFieldName fields'

layoutNames :: MonadIO m => [B.ByteString] -> Ptr CChar -> m ()
layoutNames [] _ = return ()
layoutNames (bstr:rest) ptr = liftIO $ do
  sz <- B.unsafeUseAsCStringLen bstr $ \(cstr, sz) -> do
    copyBytes ptr cstr sz
    pokeElemOff ptr sz 0
    return sz
  layoutNames rest (plusPtr ptr (sz+1))
{-# INLINE layoutNames #-}

-- | Add a cookie with timestamp and values to `TdbCons`
addCookie :: MonadIO m
          => TdbCons
          -> Cookie
          -> UnixTime
          -> [B.ByteString]
          -> m ()
addCookie _ cookie _ _ | B.length cookie /= 16 =
  error "addCookie: cookie must be 16 bytes in length."
addCookie (TdbCons mvar) cookie epoch values = liftIO $ withMVar mvar $ \case
  Nothing -> error "addCookie: tdb_cons is closed."
  Just ptr ->
    B.unsafeUseAsCString cookie $ \cookie_ptr ->
      allocaBytes bytes_required $ \values_ptr -> do
        layoutNames values values_ptr

        -- tdb_cons_add doesn't seem to have failure conditions.
        -- at least not any communicated through return value (it always
        -- returns 0).
        void $ tdb_cons_add ptr (castPtr cookie_ptr) epoch values_ptr
 where
  bytes_required = sum $ fmap ((+1) . B.length) values
{-# INLINE addCookie #-}

-- ¦ Finalizes `TdbCons`
finalizeTrailDBCons :: MonadIO m => TdbCons -> m ()
finalizeTrailDBCons (TdbCons mvar) = liftIO $ withMVar mvar $ \case
  Nothing -> error "finalizeTrailDBCons: tdb_cons is closed."
  Just ptr -> do
    result <- tdb_cons_finalize ptr 0
    unless (result == 0) $
      error "finalizeTrailDBCons: tdb_cons_finalize() failed."

-- | Close a `TdbCons`
--
-- Does nothing if it's closed already.
closeTrailDBCons :: MonadIO m => TdbCons -> m ()
closeTrailDBCons (TdbCons mvar) = liftIO $ mask_ $ modifyMVar_ mvar $ \case
  Nothing -> return Nothing
  Just ptr -> tdb_cons_free ptr >> return Nothing

makeRandomTrailDB :: MonadIO m => m ()
makeRandomTrailDB = do
  cons <- newTrailDBCons "tdb_test" ["hello" :: B.ByteString, "world"]
  replicateM_ 1000000 $ do
    addCookie cons "ABCDEFGHABCDEFGH" 3 ["nakki", "kebab"]
  finalizeTrailDBCons cons
  closeTrailDBCons cons

-- | Appends a `Tdb` to an open `TdbCons`.
appendTdbToTdbCons :: MonadIO m
                   => Tdb
                   -> TdbCons
                   -> m ()
appendTdbToTdbCons (Tdb mvar_tdb) (TdbCons mvar_tdb_cons) = liftIO $
  withMVar mvar_tdb_cons $ \case
    Nothing -> error "appendTdbToTdbCons: tdb_cons is closed."
    Just tdb_cons_ptr -> withMVar mvar_tdb $ \case
      Nothing -> error "appendTdbToTdbCons: tdb is closed."
      Just (tdbPtr -> tdb_ptr) -> do
        result <- tdb_cons_append tdb_cons_ptr tdb_ptr
        unless (result == 0) $
          error "appendTdbToTdbCons: tdb_cons_append() failed."

-- | Opens an existing TrailDB.
openTrailDB :: MonadIO m
            => FilePath
            -> m Tdb
openTrailDB root = liftIO $ mask_ $
  withCString root $ \root_str -> do
    tdb <- tdb_open root_str
    when (tdb == nullPtr) $
      throwIO CannotOpenTrailDB

    buf <- mallocForeignPtrArray 1

    -- Protect concurrent access and attach a tdb_close to garbage collector
    mvar <- newMVar (Just TdbState {
        tdbPtr = tdb
      , decodeBuffer = buf
      , decodeBufferSize = 1
      })
    void $ mkWeakMVar mvar $ modifyMVar_ mvar $ \case
      Nothing -> return Nothing
      Just (tdbPtr -> ptr) -> tdb_close ptr >> return Nothing

    return $ Tdb mvar

-- | Hints that `Tdb` will not be accessed in near future.
--
-- This has no effects on semantics, only performance.
dontneedTrailDB :: MonadIO m
                => Tdb
                -> m ()
dontneedTrailDB tdb = withTdb tdb "dontneedTrailDB" tdb_dontneed

-- | Closes a TrailDB.
--
-- Does nothing if `Tdb` is already closed.
closeTrailDB :: MonadIO m
             => Tdb
             -> m ()
closeTrailDB (Tdb mvar) = liftIO $ mask_ $ modifyMVar_ mvar $ \case
  Nothing -> return Nothing
  Just (tdbPtr -> ptr) -> tdb_close ptr >> return Nothing

decodeTrailDB :: MonadIO m
              => Tdb
              -> CookieID
              -> m [(UnixTime, [Feature])]
decodeTrailDB tdb@(Tdb mvar) cid = liftIO $ join $ modifyMVar mvar $ \case
  Nothing -> error "decodeTrailDB: tdb is closed."
  old_st@(Just st) -> do
    let ptr = tdbPtr st
    withForeignPtr (decodeBuffer st) $ \decode_buffer -> do
      result <- tdb_decode_trail ptr cid decode_buffer (fromIntegral $ decodeBufferSize st) 0
      when (result == 0) $
        throwIO NoSuchCookieID
      if result == decodeBufferSize st
        then grow st
        else do results <- process decode_buffer 0 $ fromIntegral result
                return (old_st, return results)
 where
  grow st = do
    let new_size = decodeBufferSize st * 2
    ptr <- mallocForeignPtrArray $ fromIntegral new_size
    return (Just st { decodeBufferSize = new_size
                    , decodeBuffer = ptr }
           ,decodeTrailDB tdb cid)

  process !_ n l | n >= l = return []
  process ptr n l = do
    time_stamp <- peekElemOff ptr n
    (item, new_n) <- process2 ptr (n+1) l
    ((time_stamp, item):) <$> process ptr new_n l

  process2 !_ n l | n >= l = return ([], n)
  process2 ptr n l = do
    feature <- peekElemOff ptr n
    if feature == 0
      then return ([], n+1)
      else do (lst, new_n) <- process2 ptr (n+1) l
              return $ (Feature feature:lst, new_n)
    
{-# INLINE decodeTrailDB #-}

        --results <- fmap Feature <$> peekArray (fromIntegral result) decode_buffer
        --        return (old_st, return results)

withTdb :: MonadIO m => Tdb -> String -> (Ptr TdbRaw -> IO a) -> m a
withTdb (Tdb mvar) errstring action = liftIO $ withMVar mvar $ \case
  Nothing -> error $ errstring <> ": tdb is closed."
  Just (tdbPtr -> ptr) -> action ptr
{-# INLINE withTdb #-}

-- | Finds a cookie by cookie ID.
getCookie :: MonadIO m => Tdb -> CookieID -> m Cookie
getCookie tdb cid = withTdb tdb "getCookie" $ \ptr -> do
  cptr <- tdb_get_cookie ptr cid
  when (cptr == nullPtr) $
    throwIO NoSuchCookieID
  B.packCStringLen (castPtr cptr, 16)
{-# INLINE getCookie #-}

-- | Find a cookie ID by cookie.
getCookieID :: MonadIO m => Tdb -> Cookie -> m CookieID
getCookieID _ cookie | B.length cookie /= 16 = error "getCookieID: cookie must be 16 bytes in length."
getCookieID tdb cookie = withTdb tdb "getCookieID" $ \ptr ->
  B.unsafeUseAsCString cookie $ \cookie_str -> do
    result <- tdb_get_cookie_id ptr (castPtr cookie_str)
    if result == 0xffffffffffffffff
      then throwIO NoSuchCookie
      else return result
{-# INLINE getCookieID #-}

-- | Returns the number of cookies in `Tdb`
getNumCookies :: MonadIO m => Tdb -> m Word64
getNumCookies tdb = withTdb tdb "getNumCookies" tdb_num_cookies

-- | Returns the number of events in `Tdb`
getNumEvents :: MonadIO m => Tdb -> m Word64
getNumEvents tdb = withTdb tdb "getNumEvents" tdb_num_events

-- | Returns the number of fields in `Tdb`
getNumFields :: MonadIO m => Tdb -> m Word32
getNumFields tdb = withTdb tdb "getNumFields" tdb_num_fields

-- | Returns the minimum timestamp in `Tdb`
getMinTimestamp :: MonadIO m => Tdb -> m UnixTime
getMinTimestamp tdb = withTdb tdb "getMinTimestamp" tdb_min_timestamp

-- | Returns the maximum timestamp in `Tdb`
getMaxTimestamp :: MonadIO m => Tdb -> m UnixTime
getMaxTimestamp tdb = withTdb tdb "getMaxTimestamp" tdb_max_timestamp

getFieldName :: MonadIO m => Tdb -> FieldID -> m FieldName
getFieldName tdb fid = withTdb tdb "getFieldName" $ \ptr -> do
  result <- tdb_get_field_name ptr fid
  when (result == nullPtr) $ throwIO NoSuchFieldID
  B.packCString result

getFieldID :: MonadIO m => Tdb -> FieldName -> m FieldID
getFieldID tdb field_name = withTdb tdb "getFieldID" $ \ptr ->
  B.useAsCString field_name $ \field_name_cstr -> do
    result <- tdb_get_field ptr field_name_cstr
    when (result == -1) $ throwIO NoSuchField
    return $ fromIntegral result

getValue :: MonadIO m => Tdb -> Feature -> m B.ByteString
getValue tdb (Feature ft) = withTdb tdb "getValue" $ \ptr -> do
  cstr <- tdb_get_item_value ptr ft
  when (cstr == nullPtr) $ throwIO NoSuchValue
  B.packCString cstr
{-# INLINE getValue #-}

getItem :: MonadIO m => Tdb -> FieldID -> B.ByteString -> m Feature
getItem tdb fid bs = withTdb tdb "getItem" $ \ptr -> do
  B.useAsCString bs $ \cstr -> do
    ft <- tdb_get_item ptr fid cstr
    return $ Feature ft

