
# Redis storage driver for Seneca which supports key expiry

Note that this plugin was originally forked from [Seneca Redis Store](https://github.com/bamse16/seneca-redis-store). It was forked to a new plugin because:

* the original plugin uses hset/hget under the hood
* hset doesn't support expiry of individual fields
* to support expires, a complete re-implementation of redis-store was necessary under the hood
* redis-store-expires is implemented using individual keys (i.e. set vs hset) which can optionally be expired

So our advise on which plugin to use is simply follows:

* if you don't want to expire keys use the original module (in theory it's a more efficient redis store due to the underlying hset/hget usage)
* if you do want to expire keys, use this module

# Expiry

This Redis store supports expires as follows:

* you can pass a default 'expire' property into the seneca config, e.g.

    seneca.use({host:'localhost', port:6379, expire:10})

This will cause all Entites to expire in 10 seconds.

* if no 'expire' property is passed, the default is 0 (do not expire).

* it is possible to override the 'expire' property for individual Entities by passing 'entityspec' into the configuration, e.g.

```
var entityspec = {
  '-/-/shortExpiringEntity': {
    expire: 2
  },
  '-/-/noExpiringEntity': {
    expire: 0
  }
};

seneca.use({host:'localhost', port:6379, expire:10, entityspec: entityspec})

```

In our above example, all entities will expire by default in 10 seconds, with our two exceptions above, 'shortExpiringEntity' which will expire in 2 seconds, and 'noExpiringEntity' which will not expire at all.

It's also possible to set an expire on an individual entity instance itself:

```
var expireSeneca = si.delegate({expire$: 100})
var entity = si.make('myent',{data:222, id:2})
```

In this example, the expiry for 'myent' will be 100 seconds.

Tested on: Node 0.10.24, Seneca 0.5.15, Redis 2.8.5


[Seneca]: http://senecajs.org/
