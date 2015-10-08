# Introduction #

pyorapool will be run as a pyro daemon that listens for incoming pyro client connections.
In your python project that you want to use pooled connections, you have different options of using pyorapool.

First, you can get a connection object, that you can almost use like a cx\_oracle connection object. For now it has the restriction that you can't call stored procedures directly but you have to create a wrapper inside oracle\_pool\_procedures.py.

If you use Django, there is a replacement backend in the oracle\_pyro folder so you can use pyorapool without modifications to existing code. This however doesn't allow calling connection.callproc for now (which never seemed to work correctly with Django's oracle backend anyway).