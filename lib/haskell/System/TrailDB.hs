{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | Haskell bindings to trailDB library.
--
-- The bindings are multithread-safe (a thread is blocked until another thread
-- has finished operation) and asynchronous exception-safe. Garbage collector
-- finalizers are used to clean up anything that was not manually closed.
--
-- These bindings use natively-sized integers in many functions, such as
-- `Word8` for field index. This can cause some noise in your code.
-- 
-- Some of the key functions in reading TrailDBs are `openTrailDB`,
-- `decodeTrailDB`, `getFieldID`, `getItem` and possibly `getCookie`. Many
-- other functions are either conveniences or not useful in every application.
--
-- Here's an example program using relatively efficient, but low-level
-- primitives that counts the number of 'Android' web browsers in a trailDB
-- containing a 'browser' field.
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
-- {-\# LANGUAGE BangPatterns \#-}
--
-- import qualified Data.Vector.Unbox as V
-- import System.TrailDB
--
-- countAndroids :: IO Int
-- countAndroids = withTrailDB "traildb-directory" $ \\tdb -> do
--   browser_field_id <- getFieldID tdb (\"browser\" :: String)
--   android_id <- getItem tdb browser_field_id \"Android\"
--   number_of_androids \<- flip (foldOverTrailDB tdb) 0 $ \\cookie_id trail !android_count -\> return $
--     (+android_count) $ sum $ fmap (\\(timestamp, features) -\>
--                    if features V.! fromIntegral browser_id_field == android_id
--                      then 1
--                      else 0) trail
--   return number_of_androids
-- @
--
-- And here's a second program, doing the exact same thing, but using fancy
-- (but somewhat unsafe) stuff. It's slightly slower than above program.
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
-- 
-- import Control.Exception ( evaluate )
-- import Control.Lens
-- import System.TrailDB
-- 
-- countAndroids :: IO Int
-- countAndroids = withTrailDB \"traildb-directory\" $ \\tdb -> do
--   browser_field_id <- getFieldID tdb (\"browser\" :: String)
--   android_id \<- getItem tdb browser_field_id \"Android\"
--   let answer = lengthOf (tdbFold._2.each.filtered (\crumb -> crumb^?_2.ix (fromIntegral browser_field_id) == Just android_id)) tdb
--   evaluate answer
-- @

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
  , willneedTrailDB
  , withTrailDB
  -- * Accessing TrailDBs
  -- | All the functions in this section get trails and crumbs from the TrailDB
  -- with their cookie IDs but they differ in the way you invoke them.
  -- 
  -- `decodeTrailDB` is the function to use if you want random access.
  -- `foldTrailDB` is the most efficient function to traverse the entire
  -- TrailDB in Haskell at the moment.
  , decodeTrailDB
  , foldTrailDB
  , foldOverTrailDB
  , foldOverTrailDBFile
  , foldOverTrailDBFile_
  -- ** Utilities
  , findFromTrail
  , findFromTrail_
  , findFromCrumb
  -- ** Folds, traversals
  , feature
  , timestamp
  , crumbs
  -- ** Feature manipulation
  -- | `Feature` is internally a 32-bit integer containing an 8-bit field index
  -- and a 24-bit value.
  , field
  , value
  , field'
  , value'
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
  , getItemByField
  , getValue
  , getItem
  -- ** Lazy/unsafe functions
  --
  -- | These operations can break referential transparency if used incorrectly.
  -- However, they can be rather convenient when you can use them safely as you
  -- can pretend data in a `Tdb` is in memory and can be accessed purely.
  , tdbFunction
  , tdbFold
  -- * Data types
  , Cookie
  , CookieID
  , Crumb
  , FieldID
  , FieldName
  , FieldNameLike(..)
  , Feature()
  , featureWord
  , Trail
  , TdbCons()
  , Tdb()
  , Value
  -- ** Time
  , UnixTime
  , getUnixTime
  -- * Exceptions
  , TrailDBException(..)
  -- * Multiple TrailDBs
  --
  -- | Operations in this section are conveniences that may be useful if you
  --   have many TrailDB directories you want to query.
  , findTrailDBs
  , filterTrailDBDirectories )
  where

import Control.Concurrent
import Control.Lens hiding ( coerce )
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Primitive
import Control.Monad.Trans.State.Strict
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as BL
import Data.Bits
import Data.Coerce
import Data.Foldable
import Data.Data
import Data.IORef
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import Data.Time.Clock.POSIX
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import Data.Word
import Foreign.C.String
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.StablePtr
import Foreign.Storable
import GHC.Generics
import System.Directory
import System.IO.Error
import System.IO.Unsafe
import System.Posix.Files.ByteString

import System.TrailDB.Internal

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
foreign import ccall unsafe tdb_willneed
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

foreign import ccall safe tdb_fold
  :: Ptr TdbRaw
  -> FunPtr TdbFoldFun
  -> Ptr ()
  -> IO (Ptr ())

foreign import ccall "wrapper" wrap_tdb_fold
  :: TdbFoldFun
  -> IO (FunPtr TdbFoldFun)

type TdbFoldFun = Ptr TdbRaw -> Word64 -> Ptr Word32 -> Ptr () -> IO (Ptr ())

-- | Cookies should be 16 bytes in size.
type Cookie = B.ByteString
type FieldName = B.ByteString
type UnixTime = Word32

type CookieID = Word64

type FieldID = Word8
type Value = Word32

-- | `Trail` is the list of all events (or crumbs) that have happened to a cookie.
type Trail = [Crumb]

-- | A single crumb is some event at certain time.
--
-- The vector always has length as told by `getNumFields`.
type Crumb = (UnixTime, V.Vector Feature)

-- | Exceptions that may happen with TrailDBs.
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
  | FinalizationFailure     -- ^ For some reason, finalizing a `TdbCons` failed.
  deriving ( Eq, Ord, Show, Read, Typeable, Data, Generic, Enum )

instance Exception TrailDBException

newtype Feature = Feature Word32
  deriving ( Eq, Ord, Show, Read, Typeable, Data, Generic, Storable )

featureWord :: Iso' Feature Word32
featureWord = iso (\(Feature w) -> w) (\w -> Feature w)
{-# INLINE featureWord #-}

newtype instance V.Vector Feature = V_Feature (V.Vector Word32)
newtype instance VM.MVector s Feature = VM_Feature (VM.MVector s Word32)

instance VGM.MVector VM.MVector Feature where
  {-# INLINE basicLength #-}
  basicLength (VM_Feature w32) = VGM.basicLength w32
  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice a b (VM_Feature w32) = coerce $
    VGM.basicUnsafeSlice a b w32
  {-# INLINE basicOverlaps #-}
  basicOverlaps (VM_Feature w1) (VM_Feature w2) = VGM.basicOverlaps w1 w2
  {-# INLINE basicUnsafeNew #-}
  basicUnsafeNew sz = do
    result <- VGM.basicUnsafeNew sz
    return $ VM_Feature result
  {-# INLINE basicUnsafeRead #-}
  basicUnsafeRead (VM_Feature w32) i = coerce <$> VGM.basicUnsafeRead w32 i
  {-# INLINE basicUnsafeWrite #-}
  basicUnsafeWrite (VM_Feature w32) i v =
    VGM.basicUnsafeWrite w32 i (coerce v)

instance VG.Vector V.Vector Feature where
  {-# INLINE basicLength #-}
  basicLength (V_Feature w32) = VG.basicLength w32
  {-# INLINE basicUnsafeFreeze #-}
  basicUnsafeFreeze (VM_Feature w32) = do
    result <- VG.basicUnsafeFreeze w32
    return $ coerce (result :: V.Vector Word32)
  {-# INLINE basicUnsafeThaw #-}
  basicUnsafeThaw (V_Feature w32) = do
    result <- VG.basicUnsafeThaw w32
    return $ coerce result
  {-# INLINE basicUnsafeIndexM #-}
  basicUnsafeIndexM (V_Feature w32) idx =
    fmap coerce $ VG.basicUnsafeIndexM w32 idx
  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice i1 i2 (V_Feature w32) =
    coerce $ VG.basicUnsafeSlice i1 i2 w32

-- | `Feature` is isomorphic to `Word32` so it's safe to coerce between them. (see `featureWord`).
instance V.Unbox Feature

field :: Lens' Feature FieldID
field = lens field' (\(Feature old) new -> Feature $ (old .&. 0xffffff00) .|. fromIntegral new)
{-# INLINE field #-}

value :: Lens' Feature Value
value = lens value' (\(Feature old) new -> Feature $ (old .&. 0x000000ff) .|. (new `shiftL` 8))
{-# INLINE value #-}

field' :: Feature -> FieldID
field' (Feature w) = fromIntegral $ w .&. 0x000000ff
{-# INLINE field' #-}

value' :: Feature -> Value
value' (Feature w) = w `shiftR` 8
{-# INLINE value' #-}

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

instance FieldNameLike T.Text where
  encodeToFieldName = T.encodeUtf8

instance FieldNameLike TL.Text where
  encodeToFieldName = T.encodeUtf8 . TL.toStrict

newtype TdbCons = TdbCons (MVar (Maybe (Ptr TdbConsRaw)))
  deriving ( Typeable, Generic )

-- | Create a new TrailDB and return TrailDB construction handle.
--
-- Close it with `closeTrailDBCons`. Garbage collector will close it eventually
-- if you didn't do it yourself. You won't be receiving `FinalizationFailure`
-- exception though if that fails when using the garbage collector.
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
        throwM CannotOpenTrailDBCons

      -- MVar will protect the handle from being used in multiple threads
      -- simultaneously.
      mvar <- newMVar (Just tdb_cons)

      -- Make garbage collector close it if it wasn't already.
      void $ mkWeakMVar mvar $ modifyMVar_ mvar $ \case
        Nothing -> return Nothing
        Just ptr -> do
          void $ tdb_cons_finalize ptr 0
          tdb_cons_free ptr
          return Nothing

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

-- | Add a cookie with timestamp and values to `TdbCons`.
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

-- | Finalizes a `TdbCons`.
--
-- You usually don't need to call this manually because this is called
-- automatically by `closeTrailDBCons` before actually closing `TdbCons`.
finalizeTrailDBCons :: MonadIO m => TdbCons -> m ()
finalizeTrailDBCons (TdbCons mvar) = liftIO $ withMVar mvar $ \case
  Nothing -> error "finalizeTrailDBCons: tdb_cons is closed."
  Just ptr -> do
    result <- tdb_cons_finalize ptr 0
    unless (result == 0) $ throwM FinalizationFailure

-- | Close a `TdbCons`
--
-- Does nothing if it's closed already.
closeTrailDBCons :: MonadIO m => TdbCons -> m ()
closeTrailDBCons (TdbCons mvar) = liftIO $ mask_ $ modifyMVar_ mvar $ \case
  Nothing -> return Nothing
  Just ptr -> do
    result <- tdb_cons_finalize ptr 0
    unless (result == 0) $ throwM FinalizationFailure
    tdb_cons_free ptr >> return Nothing

-- | Appends a `Tdb` to an open `TdbCons`.
appendTdbToTdbCons :: MonadIO m
                   => Tdb
                   -> TdbCons
                   -> m ()
appendTdbToTdbCons (Tdb mvar_tdb) (TdbCons mvar_tdb_cons) = liftIO $
  withMVar mvar_tdb_cons $ \case
    Nothing -> error "appendTdbToTdbCons: tdb_cons is closed."
    Just tdb_cons_ptr -> withCVar mvar_tdb $ \case
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
      throwM CannotOpenTrailDB

    buf <- mallocForeignPtrArray 1

    -- Protect concurrent access and attach a tdb_close to garbage collector
    mvar <- newCVar (Just TdbState {
        tdbPtr = tdb
      , decodeBuffer = buf
      , decodeBufferSize = 1
      })
    void $ mkWeakCVar mvar $ modifyCVar_ mvar $ \case
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

-- | Hints that `Tdb` will be walked over in near future.
--
-- This has no effects on semantics, only performance.
willneedTrailDB :: MonadIO m
                => Tdb
                -> m ()
willneedTrailDB tdb = withTdb tdb "willneedTrailDB" tdb_willneed

-- | Closes a TrailDB.
--
-- Does nothing if `Tdb` is already closed.
closeTrailDB :: MonadIO m
             => Tdb
             -> m ()
closeTrailDB (Tdb mvar) = liftIO $ mask_ $ modifyCVar_ mvar $ \case
  Nothing -> return Nothing
  Just (tdbPtr -> ptr) -> tdb_close ptr >> return Nothing

-- | Fetches a trail by `CookieID`.
--
-- TrailDB is designed to have good random access performance so mass querying
-- is somewhat practical with this function.
decodeTrailDB :: MonadIO m
              => Tdb
              -> CookieID
              -> m Trail
decodeTrailDB tdb@(Tdb mvar) cid = liftIO $ join $ modifyCVar mvar $ \case
  Nothing -> error "decodeTrailDB: tdb is closed."
  old_st@(Just st) -> do
    let ptr = tdbPtr st
    withForeignPtr (decodeBuffer st) $ \decode_buffer -> do
      result <- tdb_decode_trail ptr cid decode_buffer (fromIntegral $ decodeBufferSize st) 0
      when (result == 0) $
        throwM NoSuchCookieID
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
    ((time_stamp, V.fromList item):) <$> process ptr new_n l

  process2 !_ n l | n >= l = return ([], n)
  process2 ptr n l = do
    feature <- peekElemOff ptr n
    if feature == 0
      then return ([], n+1)
      else do (lst, new_n) <- process2 ptr (n+1) l
              return $ (Feature feature:lst, new_n)
{-# INLINE decodeTrailDB #-}

withTdb :: MonadIO m => Tdb -> String -> (Ptr TdbRaw -> IO a) -> m a
withTdb (Tdb mvar) errstring action = liftIO $ withCVar mvar $ \case
  Nothing -> error $ errstring <> ": tdb is closed."
  Just (tdbPtr -> ptr) -> action ptr
{-# INLINE withTdb #-}

-- | Finds a cookie by cookie ID.
getCookie :: MonadIO m => Tdb -> CookieID -> m Cookie
getCookie tdb cid = withTdb tdb "getCookie" $ \ptr -> do
  cptr <- tdb_get_cookie ptr cid
  when (cptr == nullPtr) $
    throwM NoSuchCookieID
  B.packCStringLen (castPtr cptr, 16)
{-# INLINE getCookie #-}

-- | Find a cookie ID by cookie.
getCookieID :: MonadIO m => Tdb -> Cookie -> m CookieID
getCookieID _ cookie | B.length cookie /= 16 = error "getCookieID: cookie must be 16 bytes in length."
getCookieID tdb cookie = withTdb tdb "getCookieID" $ \ptr ->
  B.unsafeUseAsCString cookie $ \cookie_str -> do
    result <- tdb_get_cookie_id ptr (castPtr cookie_str)
    if result == 0xffffffffffffffff
      then throwM NoSuchCookie
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
  when (result == nullPtr) $ throwM NoSuchFieldID
  B.packCString result

-- | Given a field name, returns its `FieldID`.
getFieldID :: (FieldNameLike a, MonadIO m) => Tdb -> a -> m FieldID
getFieldID tdb (encodeToFieldName -> field_name) = withTdb tdb "getFieldID" $ \ptr ->
  B.useAsCString field_name $ \field_name_cstr -> do
    result <- tdb_get_field ptr field_name_cstr
    when (result == -1) $ throwM NoSuchField
    return $ fromIntegral result-1

-- | Given a `Feature`, returns a string that describes it.
--
-- Values in a TrailDB are integers which need to be mapped back to strings to
-- be human-readable.
getValue :: MonadIO m => Tdb -> Feature -> m B.ByteString
getValue tdb (Feature ft) = withTdb tdb "getValue" $ \ptr -> do
  cstr <- tdb_get_item_value ptr ft
  when (cstr == nullPtr) $ throwM NoSuchValue
  B.packCString cstr
{-# INLINE getValue #-}

-- | Given a field ID and a human-readable value, turn it into `Feature` for that field ID.
getItem :: MonadIO m => Tdb -> FieldID -> B.ByteString -> m Feature
getItem tdb fid bs = withTdb tdb "getItem" $ \ptr -> do
  B.useAsCString bs $ \cstr -> do
    ft <- tdb_get_item ptr (fid+1) cstr
    return $ Feature ft

-- | Same as `getItem` but uses a resolved field name rather than raw `FieldID`.
--
-- This is implemented in terms of `getFieldID` and `getItem` inside.
getItemByField :: (FieldNameLike a, MonadIO m) => Tdb -> a -> B.ByteString -> m Feature
getItemByField tdb (encodeToFieldName -> fid) bs = liftIO $ do
  fid <- getFieldID tdb fid
  getItem tdb fid bs

-- | Finds a specific feature from a crumb.
findFromCrumb :: (Feature -> Bool)
              -> Crumb
              -> Maybe Feature
findFromCrumb fun (_, features) =
  loop_it 0
 where
  len = V.length features

  loop_it n | n >= len = Nothing
  loop_it n =
    let f = features V.! n
     in if fun f
          then Just f
          else loop_it (n+1)
{-# INLINE findFromCrumb #-}

-- | Finds a specific feature from a trail.
findFromTrail :: (Feature -> Bool)
              -> Trail
              -> Maybe (Crumb, Feature)
findFromTrail fun trail = loop_it trail
 where
  loop_it [] = Nothing
  loop_it (crumb:rest) =
    case findFromCrumb fun crumb of
      Nothing -> loop_it rest
      Just feature -> Just (crumb, feature)
{-# INLINE findFromTrail #-}

-- | Same as `findFromTrail` but does not return `Crumb` with the feature.
findFromTrail_ :: (Feature -> Bool)
               -> Trail
               -> Maybe Feature
findFromTrail_ fun trail = case findFromTrail fun trail of
  Nothing -> Nothing
  Just (_, feature) -> Just feature
{-# INLINE findFromTrail_ #-}

-- | Traversal to features in a trail.
feature :: Traversal' Trail Feature
feature = each._2.each
{-# INLINE feature #-}

-- | Traversal to timestamps in a trail
timestamp :: Traversal' Trail UnixTime
timestamp _ [] = pure []
timestamp fun (crumb:rest) =
  (:) <$> loop_it crumb <*> timestamp fun rest
 where
  loop_it (tm, item) = (,) <$> fun tm <*> pure item
{-# INLINE timestamp #-}

-- | Folds over an opened TrailDB.
--
-- You cannot interrupt this fold. It uses callback mechanism from C back to
-- Haskell so throwing exceptions to it can result in Bad Things.
--
-- If this scares you, use the safer (but marginally slower) `foldOverTrailDB`
-- or `foldOverTrailDB_`, which can be run in a monad stack and interrupted as
-- you please.
foldTrailDB :: Tdb
            -> (CookieID -> Crumb -> a -> IO a)
            -> a
            -> IO a
foldTrailDB (Tdb cvar) trailer initial_value = withCVar cvar $ \case
  Nothing -> error "foldTrailDB: tdb is closed."
  Just tdbstate -> mask_ $ do
    ref <- newIORef initial_value
    num_fields <- fromIntegral <$> tdb_num_fields (tdbPtr tdbstate)
    value_ptr <- newStablePtr ref
    funwrap <- wrap_tdb_fold (hfun num_fields)
    flip finally (freeStablePtr value_ptr >> freeHaskellFunPtr funwrap) $ do
      _ <- tdb_fold (tdbPtr tdbstate)
                    funwrap
                    (castStablePtrToPtr value_ptr)
      touch cvar
      readIORef ref
 where
  hfun :: Int -> TdbFoldFun
  hfun num_fields _ cid fields user_args = do
    value_ref <- deRefStablePtr $ castPtrToStablePtr user_args
    value <- readIORef value_ref
    time_stamp <- peekElemOff fields 0
    
    features_vector <- VM.unsafeNew (num_fields-1)
    for_ [1..num_fields-1] $ \field_index -> do
      feat <- Feature <$> peekElemOff fields field_index
      VM.unsafeWrite features_vector (field_index-1) feat

    frozen_vector <- V.unsafeFreeze features_vector

    new_value <- trailer cid (time_stamp, frozen_vector) value
    writeIORef value_ref new_value
    return user_args
{-# INLINE foldTrailDB #-}

-- | Convenience function that folds over all trails in a `Tdb`.
foldOverTrailDB :: MonadIO m
                => Tdb
                -> (CookieID -> Trail -> a -> m a)
                -> a
                -> m a
foldOverTrailDB tdb folder accum = do
  num_cookies <- getNumCookies tdb
  loop_it tdb folder 0 num_cookies accum
 where
  loop_it _ _ n num_cookies !accum | n >= num_cookies = return accum
  loop_it tdb folder n num_cookies !accum = do
    trail <- decodeTrailDB tdb n
    new_accum <- folder n trail accum
    loop_it tdb folder (n+1) num_cookies new_accum
{-# INLINE foldOverTrailDB #-}

-- | Same as `foldOverTrailDB` but opens `Tdb` from a file and closes it after
-- it is done.
foldOverTrailDBFile :: (MonadIO m, MonadMask m)
                    => FilePath
                    -> (Tdb -> m (CookieID -> Trail -> a -> m a)) -- ^ Return a folder for the `Tdb`. This allows you to do some setup for your folder based on `Tdb`.
                    -> a
                    -> m a
foldOverTrailDBFile fpath folder_creator initial_value =
  withTrailDB fpath $ \tdb -> do
    folder <- folder_creator tdb
    foldOverTrailDB tdb folder initial_value
{-# INLINE foldOverTrailDBFile #-}

-- | Same as `foldOverTrailDBFile` but you don't get to access `Tdb`.
--
-- This is simpler to invoke than `foldOverTrailDBFile` but lack setup may mean
-- it's difficult to make your folder look up features efficiently.
foldOverTrailDBFile_ :: (MonadIO m, MonadMask m)
                     => FilePath
                     -> (CookieID -> Trail -> a -> m a)
                     -> a
                     -> m a
foldOverTrailDBFile_ fpath folder initial_value =
  withTrailDB fpath $ \tdb -> foldOverTrailDB tdb folder initial_value
{-# INLINE foldOverTrailDBFile_ #-}

-- | Dive into crumbs in trail.
crumbs :: Traversal' Trail Crumb
crumbs = each
{-# INLINE crumbs #-}

-- | Returns a pure function that returns trails from `Tdb`.
--
-- This function can be unsafe. If you close the `Tdb` and still use the
-- returned function, the function will break referential transparency by
-- throwing an exception. Same thing can happen if something happens to the
-- `Tdb` in the Real World, modifying results.
--
-- However, the purity can be a major convenience if your `Tdb` is stable and
-- you won't close it manually.
tdbFunction :: Tdb -> (CookieID -> Maybe Trail)
tdbFunction tdb cid = unsafePerformIO $ do
  catch (Just <$> decodeTrailDB tdb cid)
        (\NoSuchCookieID -> return Nothing)
{-# INLINE tdbFunction #-}

-- | Returns a pure `Fold` for `Tdb`. Same caveats as listed in `tdbFunction` apply.
tdbFold :: Fold Tdb (CookieID, Trail)
tdbFold fun tdb = unsafePerformIO $ do
  cookies <- getNumCookies tdb
  if cookies == 0
    then return $ pure tdb
    else do first_trail <- unsafeInterleaveIO $ decodeTrailDB tdb 0
            let first_result = fun (0, first_trail)
            (first_result *>) <$> unsafeInterleaveIO (loop_it 1 cookies)
 where
  loop_it n cookies | n >= cookies = return (pure tdb)
  loop_it n cookies = do
    trail <- unsafeInterleaveIO $ decodeTrailDB tdb n
    (fun (n, trail) *>) <$> unsafeInterleaveIO (loop_it (n+1) cookies)
{-# INLINE tdbFold #-}

-- | Opens a `Tdb` and then closes it after action is over.
withTrailDB :: (MonadIO m, MonadMask m) => FilePath -> (Tdb -> m a) -> m a
withTrailDB fpath action = mask $ \restore -> do
  tdb <- openTrailDB fpath
  finally (restore $ action tdb) (closeTrailDB tdb)
{-# INLINE withTrailDB #-}

-- | Given a directory, find all valid TrailDB paths inside it, recursively.
findTrailDBs :: forall m. (MonadIO m, MonadMask m)
             => FilePath
             -> Bool            -- ^ Follow symbolic links?
             -> m [FilePath]
findTrailDBs filepath follow_symbolic_links = do
  contents <- liftIO $ getDirectoryContents filepath
  dirs <- execStateT (filterChildDirectories filepath contents) [filepath]
  filterTrailDBDirectories dirs
 where
  filterChildDirectories :: FilePath -> [FilePath] -> StateT [FilePath] m ()
  filterChildDirectories prefix (".":rest) = filterChildDirectories prefix rest
  filterChildDirectories prefix ("..":rest) = filterChildDirectories prefix rest
  filterChildDirectories prefix (dir_raw:rest) = do
    let dir = prefix <> "/" <> dir_raw
    is_dir <- liftIO $ doesDirectoryExist dir
    is_symbolic_link_maybe <- liftIO $ tryIOError $ getFileStatus (T.encodeUtf8 $ T.pack dir)
    case is_symbolic_link_maybe of
      Left exc | isDoesNotExistError exc -> filterChildDirectories prefix rest
      Left exc -> throwM exc
      Right is_symbolic_link ->
        if is_dir
          then (if (isSymbolicLink is_symbolic_link && follow_symbolic_links) ||
                   (not $ isSymbolicLink is_symbolic_link)
                  then modify (dir:) >> recurse dir >> filterChildDirectories prefix rest
                  else filterChildDirectories prefix rest)
          else filterChildDirectories prefix rest
  filterChildDirectories _ [] = return ()

  recurse dir = do
    contents <- liftIO $ getDirectoryContents dir
    filterChildDirectories dir contents

-- | Given a list of directories, filters it, returning only directories that
-- are valid TrailDB directories.
--
-- Used internally by `findTrailDBs` but is useful in general so we export it.
filterTrailDBDirectories :: (MonadIO m, MonadMask m) => [FilePath] -> m [FilePath]
filterTrailDBDirectories = filterM $ \dir -> do
  result <- try $ openTrailDB dir
  case result of
    Left CannotOpenTrailDB -> return False
    Left exc -> throwM exc
    Right ok -> closeTrailDB ok >> return True

