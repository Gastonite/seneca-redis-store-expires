/*jslint node: true */
/*global describe:true, it:true*/
/* Copyright (c) 2012 Marius Ursache */

'use strict';

var util = require('util');
var seneca = require('seneca');
//var async = require('async');
var shared = require('seneca-store-test');
var assert = require('assert');
var debug = require('debug')('redis.test');

var si = seneca();

// for our 'expiryexample' entity, we set a custom ttl of 2
var entityspec = {
  '-/-/expiryexample': {
    expire: 2
  }
};

si.use('..', { host:'localhost', port:6379, entityspec: entityspec, expire:7});
si.__testcount = 0;
var testcount = 0;
var timeout = 5000;
describe('redis', function(){

  describe('Expires', function(){
    it('should set and get a key/value pair', function(done){
      var entity = si.make('examplekv',{data:111, id:1});
      entity.save$(function(err,result){
        assert.ok(!err);
        assert(result);
        assert.equal(result.data,111);
        assert.equal(result.id,1);

        entity.load$({id:1},function(err,result2){
          assert(result2);
          assert.equal(result2.id,1);
          assert.equal(result2.data,111);

          done();
        });
      });
    });

    it('should delete a key', function(done){
      var entity = si.make('exampledel');
      entity.remove$({id:1},function(err,result){
        assert.ok(!err);
        debug('result: %o', result);
        // Test that key has gone
        entity.load$({id:1},function(err,result){
          assert.ok(!err);
          assert(!result);
          done();
        });
      });
    });

    it('should have empty list for no entities', function(done){
      var ex = si.make('exampleempty');
      ex.list$({}, function(err, results) {
        assert.ok(!err);
        assert.equal(results.length, 0);
        done();
      });
    });

    it('should do everything', function(done){
      //var expireSeneca = si.delegate({expire$: 100})
      var entity = si.make('everythingtest',{data:222, id:2});
      entity.save$(function(err,result){
        assert.ok(!err);
        assert(result);
        assert.equal(result.data,222);
        assert.equal(result.id,2);

        entity.load$({id:2}, function(err, result) {
          assert.ok(!err);

          entity.list$({}, function(err, results) {
            assert.ok(!err);
            assert.ok(results.length > 0);

            entity.remove$({id:2}, function(err) {
              assert.ok(!err);

              entity.load$({id:2}, function(err, result) {
                assert.ok(!err);
                assert.ok(!result);

                entity.list$({}, function(err, results) {
                  assert.ok(!err);
                  assert.equal(results.length, 0, results);
                  done();
                });
              });
            });
          });
        });
      });
    });
  });

  it('should set and get an expiry for key/value pair', function(done){
    this.timeout(10000);
    var entity = si.make('expiryexample', {data:222, id:2});
    entity.save$(function(err,result){
      assert.ok(!err);
      assert.ok(result);

      entity.load$({id:2},function(err,result){
        assert.ok(!err);
        assert.ok(result);

        setTimeout(function(){
          entity.load$({id:2},function(err,result) {
            assert.ok(!err);
            assert.ok(!result, 'No result expected, should of expired!!');
            done();
          });
        }, 6000);
      });
    });
  });

  describe('original', function(){
    // list and remove aren't implemented so won't work
    it('basic', function(done){
      this.timeout(timeout);
      testcount++;
      shared.basictest(si, done);
    });

    it('extra', function(done){
      testcount++;
      extratest(si,done)
    });

    it('close', function(done){
      this.timeout(timeout);
      shared.closetest(si, testcount, done);
    });
  });

});


function extratest(si,done) {
  console.log('EXTRA');

  var fooent = si.make('foo',{a:1});
  fooent.save$(function(err,out){
    if(err) return done(err);

    console.log(out);
    done()
  });

  si.__testcount++;
}
