{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent    (forkIO, threadDelay)
import           Control.Exception     (bracket, catch)
import           Control.Monad
import           Data.Word
import           Database.MySQL.Simple
import           Network.Socket
import           System.Environment
import qualified System.IO.Streams as Stream
import           Test.Tasty.HUnit

main :: IO ()
main = do
	putStrLn "reading env..."
	host <- getEnv "RS_HOST"
	port <- getEnv "RS_PORT"
	user <- getEnv "RS_USERNAME"
	password <- getEnv "RS_PASSWORD"
	database <- getEnv "RS_DATABASE"

	putStrLn "connecting..."
	c <- Database.MySQL.Simple.connect defaultConnectInfo{
		connectHost = host,
		connectPort = read port :: Word16,
		connectUser = user,
		connectPassword = password,
		connectDatabase = database
	}

	putStrLn "preparing table..."
	execute_ c "DROP TABLE IF EXISTS people"
	execute_ c "CREATE TABLE people (\
		\id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
		\name VARCHAR(255) NOT NULL\
	\) CHARACTER SET utf8"

	resetTestTable c

	putStrLn "INSERT..."
	execute c "INSERT INTO people (name) VALUES (?)" (Only ("Test1" :: String))

	putStrLn "SELECT..."
	result <- query_ c "SELECT * FROM people"
	assertEqual "SELECT 1 matches" result [(1 :: Int, "Test1" :: String)]

	putStrLn "INSERT..."
	execute c "INSERT INTO people (name) VALUES (?)" (Only ("Test2" :: String))

	putStrLn "SELECT..."
	result <- query_ c "SELECT * FROM people"
	assertEqual "SELECT 2 matches" result [
		(1 :: Int, "Test1" :: String),
		(2 :: Int, "Test2" :: String) ]

	putStrLn "UPDATE..."
	execute c "UPDATE people SET name = ? WHERE id = ?" ("Test2a" :: String, 2 :: Int)

	putStrLn "SELECT..."
	result <- query_ c "SELECT * FROM people"
	assertEqual "SELECT 3 matches" result [
		(1 :: Int, "Test1" :: String),
		(2 :: Int, "Test2a" :: String) ]
	
	putStrLn "SELECT ... WHERE id ..."
	result <- query c "SELECT * FROM people WHERE id = ?" (Only (2 :: Int))
	assertEqual "SELECT 4 matches" result [(2 :: Int, "Test2a" :: String)]

	putStrLn "DELETE..."
	execute c "DELETE FROM people WHERE id = ?" (Only (1 :: Int))
	
-- TODO:  Receive empty resultset and compare with expected
--	putStrLn "SELECT ... WHERE id ..."
--	result <- query c "SELECT * FROM people WHERE id = ?" (Only (1 :: Int))
--	assertEqual "SELECT 5 empty" result []

	putStrLn "SELECT..."
	result <- query_ c "SELECT * FROM people"
	assertEqual "SELECT 6 matches" result [(2 :: Int, "Test2a" :: String)]

	Database.MySQL.Simple.close c

-- TODO:  Do we care about precise authentication error text?
--	let loginFailMsg = "ERRException (ERR {errCode = 1045, errState = \"28000\", \
--		    \errMsg = \"Access denied for user 'testMySQLHaskell'@'localhost' (using password: YES)\"})"
--
--	catch
--		(void $ connectDetail defaultConnectInfo{
--			ciHost = host,
--			ciPort = read port :: PortNumber,
--			ciUser = pack user,
--			ciPassword = "wrongpassword",
--			ciDatabase = pack database
--		})
--		(\ (e :: ERRException) -> assertEqual "wrong password should fail to login" (show e) loginFailMsg)

  where
	resetTestTable c = do
		execute_ c  "TRUNCATE TABLE people"

