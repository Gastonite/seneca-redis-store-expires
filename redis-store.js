/*jslint node: true */
/*
 /* Copyright (c) 2012 Marius Ursache
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

"use strict";

var async = require('async');
var util = require('util');
var assert = require("assert");
var _ = require('lodash');
var redis = require('redis');
var uuid = require('node-uuid');
var debug = require('debug')('redis-store');

var NAME = "redis-store";
var MIN_WAIT = 16;
var MAX_WAIT = 65336;
var OBJECT_TYPE_STATIC = 's';
var OBJECT_TYPE_OBJECT = 'o';
var OBJECT_TYPE_DATE = 'd';

var globalObjectMap = {};

module.exports = function(opts) {
  var seneca = this;
  var desc;
  var minwait;
  var dbConn = null;
  var connectSpec = null;
  var waitmillis = MAX_WAIT;

  opts.minwait = opts.minwait || MIN_WAIT;
  opts.maxwait = opts.maxwait || MAX_WAIT;

  // This is how the pattern matching for the entityspec works:
  // - uses the patrun module (part of seneca)
  // - create a canonical form of each entity key, e.g. -/-/foo
  // - assicate each canonical object with the entity value
  // e.g. { '-/-/foo' : { expire: 20} }
  // Then down in save() below, we query patrun with the canonical form
  // of the entity being saved, and pull out its entity specific properties.
  var patrun = this.util.router();
  _.each(opts.entityspec, function(v, k) {
    var can = seneca.util.parsecanon(k);
    patrun.add(can, v);
  });
  debug('opts: %o', opts);

  /**
   * check and report error conditions seneca.fail will execute the callback
   * in the case of an error. Optionally attempt reconnect to the store depending
   * on error condition
   */
  function error(args, err, cb) {
    if( err ) {
      seneca.log.debug('error: '+err);
      seneca.fail({code:'entity/error',store: NAME},cb);

      if( 'ECONNREFUSED' === err.code || 'notConnected' === err.message || 'Error: no open connections' === err ) {
        minwait = opts.minwait;
        if (minwait) {
          //collmap = {};
          reconnect(args);
        }
      }
      return true;
    }
    return false;
  }


  /**
   * attemp to reconnect to the store
   */
  //TODO: this is a function of the fw / subsystem NOT the store driver
  //      drivers should be dumb!
  function reconnect(args) {
    configure(connectSpec, function(err, me) {
      if (err) {
        seneca.log(null, 'db reconnect (wait ' + waitmillis + 'ms) failed: ' + err);
        waitmillis = Math.min(2 * waitmillis, MAX_WAIT);
        setTimeout(
          function(){
            reconnect(args);
          }, waitmillis);
      }
      else {
        waitmillis = MIN_WAIT;
        seneca.log(null, 'reconnect ok');
      }
    });
  }



  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */
  function configure(spec, cb) {
    assert(spec);
    assert(cb);

    var conf = spec;
    connectSpec = spec;

    if (_.isString(spec)) {
      conf = {};
      var urlM = /^redis:\/\/((.*?)@)?(.*?)(:?(\d+))$/.exec(spec);
      conf.host = urlM[3];
      conf.password = urlM[2];
      conf.port = urlM[5];
      conf.port = conf.port ? parseInt(conf.port,10) : null;
    }

    dbConn = redis.createClient(conf.port, conf.host);
    dbConn.on('error', function(err){
      seneca.fail({code: "seneca-redis/configure", message: err.message});
    });

    if(_.has(conf, 'auth') && !_.isEmpty(conf.auth)){
      dbConn.auth(conf.auth, function(err, message){
        seneca.log({tag$:'init'}, 'authed to ' + conf.host);
      });
    }

    seneca.log({tag$:'init'}, 'db '+conf.host+' opened.');
    seneca.log.debug('init', 'db open', spec);
    cb(null);
  }


  /**
   * the simple db store interface returned to seneca
   */
  var store = {
    name: NAME,



    /**
     * close the connection
     *
     * params
     * cmd - optional close command parameters
     * cb - callback
     */
    close: function(cmd, cb) {
      assert(cb);
      if (dbConn) {
        // close the connection
        dbConn.quit();
        dbConn = null;
      }
      cb(null);
    },



    /**
     * save the data as specified in the entity block on the arguments object
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * args.expire - number of TTL seconds. If not present then looks to args.ent._expire
     * args.expireAt - number of TTL seconds. If not present then looks to args.ent._expireAt
     * cb - callback
     */
    save: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.ent);

      var ent = args.ent;

      var canon = ent.canon$({object:true});
      var entProps = patrun.find(canon);
      var expire = entProps && entProps.hasOwnProperty('expire') ? entProps.expire : (opts.expire || 0);
      var expireAt = entProps && entProps.hasOwnProperty('expireAt') ? entProps.expireAt : (opts.expireAt);

      if (args.hasOwnProperty('expire$')) expire = args.expire$;
      if (args.hasOwnProperty('expireAt$')) expireAt = args.expireAt$;

      if (expireAt) {

      }
      if (!ent.id) {
        if( ent.id$ ) {
          ent.id = ent.id$;
        }
        else {
          ent.id = uuid();
        }
      }
      debug('save - ent: %o expire: %d', ent, expire);

      var key = redisKeyForEntity(ent);
      var entp = makeentp(ent);
      debug('save - key: %s', key);

      var objectMap = determineObjectMap(ent);
      dbConn.set(key, entp, function(err, result) {
        if (!error(args, err, cb)) {
          saveMap(dbConn, objectMap, function(err, result) {


            const setExpiration = function (method, value) {
              dbConn[method](key, value, function(err, result) {
                if(err) {
                  debug(err);
                  seneca.log.error(method+' failed', key, value, err)
                }
                debug(method+' of %d successfully set', value);

                seneca.log(args.tag$,'save', result);
                cb(null, ent);
              });
            }

            if (!error(args, err, cb)) {

              if (expireAt)
                return setExpiration('expireAt', expireAt);

              if(!expire || expire === 0) {
                seneca.log(args.tag$,'save', result);
                return cb(null, ent);
              }

              setExpiration('expire', expire);
            }
          });
        }
      });
    },



    /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    load: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);
      assert(args.q.id);

      var qent = args.qent;
      var q = _.clone(args.q);
      var table = tablename(qent);
      qent.id = q.id;
      var key = redisKeyForEntity(qent);

      debug('load - quent: %o q: %o table: %s key: %s', qent, q, table, key);

      q.limit$ = 1;
      loadMap(dbConn, table, function(err, objMap) {
        if (!error(args, err, cb)) {
          dbConn.get(key, function(err, row) {
            if (!error(args, err, cb)) {
              if (!row) {
                debug('load - cache miss!');
                cb(null, null);
              }
              else {
                var ent = makeent(qent, row, objMap);
                seneca.log(args.tag$, 'load', ent);
                debug('load - cache hit! ent: %o', ent);
                cb(null, ent);
              }
            }
          });
        }
      });
    },

    /**
     * return a list of object based on the supplied query, if no query is supplied
     * then all items are selected
     *
     * Notes: trivial implementation and unlikely to perform well due to list copy
     *        also only takes the first page of results from simple DB should in fact
     *        follow paging model
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * a=1, b=2 simple
     * next paging is optional in simpledb
     * limit$ ->
     * use native$
     */
    list: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var table = tablename(qent);
      debug('list - quent: %o q: %o table: %s', qent, q, table);

      loadMap(dbConn, table, function(err, objMap) {
        if (!error(args, err, cb)) {
          dbConn.keys(table + '*', function(err, keys) {
            if (!error(args, err, cb)) {
              debug('list - keys: %o', keys);
              // Note: we don't use 'mget' here as it seemed very flaky
              // in testing - get the values one by one instead.
              // TODO - this is obviously suboptimal, needs a revisit
              getKeyValues(dbConn, keys, function(err, results) {
                if (!error(args, err, cb)) {
                  var list = [];
                  _.each(results, function(value, key) {
                    if (value) {
                      var ent = makeent(qent, value, objMap);
                      list.push(ent);
                    }
                  });

                  if (!_.isEmpty(q)) {
                    list = _.filter(list, function(elem, b, c) {
                      var match = true;
                      _.each(q, function(value, key) {
                        var computed = (elem[key] === value);
                        match = match && computed;
                      });
                      return match;
                    });
                  }
                  debug('list - returning: %o', list);
                  cb(null, list);
                }
              });
            }
          });
        }
      });
    },



    /**
     * delete an item - fix this
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */
    remove: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = _.clone(args.q);
      var table = tablename(qent);
      debug('remove - quent: %o q: %o table: %s', qent, q, table);

      if (q.all$) {
        dbConn.keys(table + '*', function(err, keys) {
          if (!error(args, err, cb)) {
            debug('remove - deleting all keys: %o', keys);
            dbConn.del(keys, function(err, result) {
              if(err){
                debug(err);
                return cb(err);
              }
              if (!error(args, err, cb)) {
                return cb(null, result);
              }
            });
          }
        });
      }else {
        store.list(args, function(err, elements) {
          if (!error(args, err, cb)) {
            var delKeys = _.map(elements, function(e) {
              return table + '_' + e.id;
            });

            debug('remove - deleting keys: %o', delKeys);
            if (delKeys.length > 0) {
              dbConn.del(delKeys, function(err, result) {
                if(err){
                  debug(err);
                  return cb(err);
                }
                if (!error(args, err, cb)) {
                  return cb(null, result);
                }
              });
            }
            else {
              cb(null, null);
            }
          }
        });
      }
    },



    /**
     * return the underlying native connection object
     */
    native: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.ent);
      debug('native - args: %o', args);
      var ent = args.ent;
      cb(null, dbConn);
    }
  };



  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;
  seneca.add({init:store.name,tag:meta.tag}, function(args,done) {
    configure(opts, function(err) {
      if (err) {
        return seneca.fail({code:'entity/configure', store:store.name, error:err, desc:desc}, done);
      }
      else done();
    });
  });
  return { name:store.name, tag:meta.tag };
};



/* ----------------------------------------------------------------------------
 * supporting boilerplate */

function getKeyValues(conn, keys, cb) {
  function getVal(key, cb) {
    conn.get(key, cb);
  }
  async.map(keys, getVal, cb);
}

var tablename = function (entity) {
  var canon = entity.canon$({object:true});
  var typeStr = (canon.base?canon.base+'_':'')+canon.name;
  return typeStr;
};

var redisKeyForEntity = function(entity) {
  var entityTypeStr = tablename(entity);
  return entityTypeStr + '_' + entity.id;
};



var makeentp = function(ent) {
  var entp = {};
  var fields = ent.fields$();

  fields.forEach(function(field){
    if(_.isDate(ent[field]) || _.isObject(ent[field])) {
      entp[field] = JSON.stringify(ent[field]);
    }
    else {
      entp[field] = ent[field];
    }
  });
  return JSON.stringify(entp);
};



var makeent = function(ent, row, objMap) {
  var entp;
  var fields;

  row = JSON.parse(row);
  fields = _.keys(row);

  if (!_.isUndefined(ent) && !_.isUndefined(row)) {
    entp = {};
    fields.forEach(function(field) {
      if (!_.isUndefined(row[field]) && !_.isUndefined(objMap.map[field])) {
        if (objMap.map[field] === OBJECT_TYPE_STATIC) {
          entp[field] = row[field];
        }
        else if (objMap.map[field] === OBJECT_TYPE_OBJECT) {
          entp[field] = JSON.parse(row[field]);
        }
        else if (objMap.map[field] === OBJECT_TYPE_DATE) {
          entp[field] = new Date(JSON.parse(row[field]));
        }
      }
    });
  }
  return ent.make$(entp);
};



var determineObjectMap = function(ent){
  var fields = ent.fields$();
  var objectName = tablename(ent);

  var objectMap = {};
  var map = {};

  fields.forEach(function(field){
    if (_.isDate(ent[field])) {
      map[field] = OBJECT_TYPE_DATE;
    }
    else if (_.isObject(ent[field])) {
      map[field] = OBJECT_TYPE_OBJECT;
    }
    else {
      map[field] = OBJECT_TYPE_STATIC;
    }
  });

  objectMap = {id:objectName, map:map};

  if(!_.isUndefined(globalObjectMap[objectName])  && _.size(objectMap.map) < _.size(globalObjectMap[objectName].map)) {
    objectMap = globalObjectMap[objectName];
  }
  else {
    globalObjectMap[objectName] = objectMap;
  }

  return objectMap;
};

var loadMap = function(dbConn, key, cb){
  var table  = 'seneca_object_map';

  dbConn.hget(table, key, function(err, result){
    if (!err) {
      var objectMap = JSON.parse(result);
      cb(null, objectMap);
    }
    else {
      cb(err, null);
    }
  });
};

var saveMap = function(dbConn, newObjectMap, cb) {

  var table  = 'seneca_object_map';
  var key = newObjectMap.id;

  loadMap(dbConn, key, function(err, existingObjMap) {
    if(err) { return cb(err, undefined); }

    var mergedObjectMap = newObjectMap;

    if(existingObjMap && existingObjMap.map) {
      mergedObjectMap = existingObjMap;
      for(var attr in newObjectMap.map) {
        if(newObjectMap.map.hasOwnProperty(attr)) {
          mergedObjectMap.map[attr] = newObjectMap.map[attr]
        }
      }
    }

    var savedObject = JSON.stringify(mergedObjectMap);
    dbConn.hset(table, key, savedObject, cb);

  });
};
