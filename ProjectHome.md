pyorapool is a connection pooling daemon written in python and pyro and can be used to keep opened connections for systems that need multiple connections quickly. It was written to be used with web frameworks like Django but is mostly independent of it.

Django version 1.2 does not keep any connections between requests but creates a new one for every query which can make it pretty slow.

pyorapool will keep connections open and will automatically reuse them again when another query is made through it. Connection timeout and error cases are handled automatically.

pyorapool includes a django database backend that can be used as a replacement for the django oracle backend which will use pooled connections.

pyorapool has been developed by [tesobe.com](http://tesobe.com/) and is in active daily use on musicpictures.com.