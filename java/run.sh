#! /bin/bash
DBNAME=jsala054_DB
PORT=9017
USER=jsala054

# Example: source ./run.sh flightDB 5432 user
java -cp lib/*:bin/ DBproject $DBNAME $PORT $USER
