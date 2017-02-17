# ratpack-memcache-client
This is a non-blocking memcache client for [Ratpack] (https://ratpack.io/) built using [Netty] (http://netty.io/) codecs.

It's designed to be:
- *efficient*: uses the same EventLoop as Ratpack and pools connected Channels.
- *natural*: the api is exposed using Ratpack's asynchronous types - Promise and Operation.
- *simple*: the client exposes a minimal set of operations but adding new operations is trivial.
