# Sprak-akka-http-couchbase-starter-kit

Main goal of this project to show an example on how to use Spark ,Akka-http along with Couchbase by making a REST Service.

This project will hep you to make CRUD operations using your own couchbase and Spark installations.

#Steps for running the Project:

1. Make two buckets i.e userBucket and testUserBucket.
2. Make View using the userddoc.ddoc file present in resources. 
3. Make a primary index on both the buckets i.e userBucket and testUserBucket.
4. Now if you want to run the test case execute "./activator clean compile test"
5. If you want to run the project execute "./activator clean compile run"
