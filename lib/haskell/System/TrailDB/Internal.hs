{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE CPP #-}

module System.TrailDB.Internal
  ( Tdb(..)
  , TdbState(..)
  , TdbConsRaw
  , TdbRaw
  
  , CVar
  , newCVar
  , mkWeakCVar
  , modifyCVar_
  , modifyCVar
  , withCVar )
  where

#ifdef USE_IOREF
import Data.IORef
#else
import Control.Concurrent.MVar
#endif
import Data.Data
import Data.Word
import Foreign.ForeignPtr
import Foreign.Ptr
import GHC.Generics
import System.Mem.Weak

#ifdef USE_IOREF
type CVar = IORef
#else
type CVar = MVar
#endif

newCVar :: a -> IO (CVar a)
#ifdef USE_IOREF
newCVar thing = newIORef thing
#else
newCVar thing = newMVar thing
#endif

mkWeakCVar :: CVar a -> IO () -> IO (Weak (CVar a))
#ifdef USE_IOREF
mkWeakCVar cvar finalizer = mkWeakIORef cvar finalizer
#else
mkWeakCVar cvar finalizer = mkWeakMVar cvar finalizer
#endif
{-# INLINE mkWeakCVar #-}

modifyCVar_ :: CVar a -> (a -> IO a) -> IO ()
#ifdef USE_IOREF
modifyCVar_ cvar action = do
  item <- readIORef cvar
  result <- action item
  writeIORef cvar result
#else
modifyCVar_ = modifyMVar_
#endif
{-# INLINE modifyCVar_ #-}

modifyCVar :: CVar a -> (a -> IO (a, b)) -> IO b
#ifdef USE_IOREF
modifyCVar cvar action = do
  item <- readIORef cvar
  (new_state, result) <- action item
  writeIORef cvar new_state
  return result
#else
modifyCVar = modifyMVar
#endif
{-# INLINE modifyCVar #-}

withCVar :: CVar a -> (a -> IO b) -> IO b
#ifdef USE_IOREF
withCVar cvar action = do
  item <- readIORef cvar
  action item
#else
withCVar cvar action = withMVar cvar action
#endif

data TdbConsRaw
data TdbRaw

data TdbState = TdbState
  { tdbPtr :: {-# UNPACK #-} !(Ptr TdbRaw)
  , decodeBuffer :: {-# UNPACK #-} !(ForeignPtr Word32)
  , decodeBufferSize :: {-# UNPACK #-} !Word32 }

newtype Tdb = Tdb (CVar (Maybe TdbState))
  deriving ( Typeable, Generic )

