GlacierConnector
================

A modeshape connector to access files in Glacier

It use the local file system as the file cache. At the first time of requestiong downloading a Glacier file, it would return exception to user but a downloading thread lanuched in the backend.
Once the job done, it would send out a notice like email or JMS message.
