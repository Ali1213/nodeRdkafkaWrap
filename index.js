const Kafka = require('node-rdkafka');
const config = require('./setting');
const util = require('util');
const path = require('path');
const PRIVATES = {
  sendMsg: Symbol('push message to kafka'),
  reconnect: Symbol('reconnect'),
};

class Producer {
  constructor({
    bootstrapServers = config.bootstrap_servers,
    username = config.sasl_plain_username,
    password = config.sasl_plain_password,
    retries = 10,
    topicName = config.topic_name,
    certPath = path.join(__dirname,'ca-cert'),
  } = {}) {
    this.bootstrap_servers = bootstrapServers;
    this.username = username;
    this.password = password;
    this.retries = retries;
    this.topic_name = topicName;
    // this.cacheObj;
    this.NOTIFY = {};

    this.messageList = [];
    this.status = 'init';
    this.onProcess = false;
    this.errorRetries = 3;
    this.errorNowTry = 0;
  }
  init() {
    this.onProcess = true;
    return new Promise((rs, rj) => {
      const self = this;
      this.cacheObj = new Kafka.Producer({
        /* 'debug': 'all', */
        'api.version.request': 'true',
        'bootstrap.servers': this.bootstrap_servers,
        'dr_cb': true,
        'dr_msg_cb': true,
        'security.protocol': 'sasl_ssl',
        // 'ssl.ca.location': '/Users/yuali/Desktop/work/task-publisher/imports/services/Producer/ca-cert',
        'ssl.ca.location': certPath,
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': this.username,
        'sasl.password': this.password,
        'message.send.max.retries': this.retries,
      });
      // Poll for events every 100 ms
      this.cacheObj.setPollInterval(100);
      // Connect to the broker manually
      this.cacheObj.connect(err => {
        if (err) {
          rj(err);
        }
        return;
      });
      this.cacheObj.on('ready', () => {
        if (this.status === 'init') {
          rs();
        }
        this.status = 'on';
        console.log('ready');
      });

      this.cacheObj.on('delivery-report', (err, report) => {
        // 消息发送成功，这里会收到report
        if (err) {
          // console.log("delivery-report: error")
          console.log(err);
          this.status = 'error';
          this.reconnect().then(() => {
            self.status = 'on';
          });
          return;
        }
        console.log(report.value.toString());
        this.notify(new Buffer(report.value).toString());
      });

      this.cacheObj.on('event.error', err => {
        if (err.code === -1) {
          this.errorNotify();
          if (this.status === 'on') {
            this.reconnect().catch(e => {
              console.log('=======重连失败');
              console.log(e);
            });
          }
          this.status = 'ing';
          return;
        }
        console.log(err);
      });
    });
  }
  errorNotify(Id) {
    if (Id) {
      this.notify(Id);
      return;
    }
    Object.keys(this.NOTIFY).forEach(item => {
      if (typeof this.NOTIFY[item] === 'function') {
        this.NOTIFY[item]('connect error');
      }
    });
    this.NOTIFY = {};
    return;
  }
  notify(str) {
    let cb;
    let kafkaId;
    try {
      kafkaId = JSON.parse(str).KAFKAID;
      cb = this.NOTIFY[kafkaId];
    } catch (e) {
      kafkaId = str;
      cb = this.NOTIFY[str];
    }
    if (typeof cb === 'function') {
      this.NOTIFY[kafkaId] = undefined;
      cb(null, str);
    }
  }
  async pushMsgs(msgs) {
    if (Array.isArray(msgs)) {
      return Promise.all(msgs.map(msg => this.pushMsg(msg)));
    }
    return this.pushMsg(msgs);
  }
  async pushMsg(msg) {
    return (util.promisify(this.pushMsgCb).bind(this))(msg);
  }
  handleMsgList() {
    if (this.messageList.length === 0) return null;
    const o = this.messageList.shift();
    this.pushMsgCb(o.msg, o.cb);
    return this.handleMsgList();
  }
  async pushMsgCb(msg, cb) {
    if (this.status === 'init') {
      if (this.onProcess) {
        this.messageList.push({
          msg,
          cb,
        });
      }
      await this.init();
      this.onProcess = false;
      this.handleMsgList();
    }
    const msgHandled = Producer.msgHandle(msg);

    this.NOTIFY[msgHandled.KAFKAID] = cb;
    await this[PRIVATES.sendMsg](JSON.stringify(msgHandled));
  }
  static msgHandle(msg) {
    let newMsg;
    const KAFKAID = Producer.randomID();

    if (Array.isArray(msg) || typeof msg === 'string') {
      newMsg = { msg, KAFKAID };
    } else if (typeof msg === 'object') {
      msg.KAFKAID = KAFKAID;
      newMsg = msg;
    } else {
      throw Error('Msg type is not support now!');
    }
    return newMsg;
  }
  [PRIVATES.sendMsg](msgHandled) {
    return new Promise((rs, rj) => {
      try {
        this.cacheObj.produce(
          // Topic to send the message to
          this.topic_name,
          // optionally we can manually specify a partition for the message
          // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
          null,
          // Message to send. Must be a buffer
          new Buffer(msgHandled),
          // for keyed messages, we also specify the key - note that this field is optional
          // 'Ali',
          undefined,
          // you can send a timestamp here. If your broker version supports it,
          // it will get added. Otherwise, we default to 0
          Date.now()
          // you can send an opaque token here, which gets passed along
          // to your delivery reports
        );
        rs();
      } catch (err) {
        console.error('A problem occurred when sending our message');
        console.error(err);
        rj(err);
      }
    });
  }
  async reconnect() {
    if (++this.errorNowTry <= this.errorRetries) {
      try {
        await this[PRIVATES.reconnect]();
        this.errorNowTry = 0;
        return Promise.resolve();
      } catch (e) {
        return this.reconnect();
      }
    }
    return Promise.reject(`Reconnect times is suplus the limit:${this.errorRetries}`);
  }
  async [PRIVATES.reconnect]() {
    await this.kafkaDisConnect();
    await this.kafkaConnect();
  }
  kafkaDisConnect() {
    return new Promise((rs, rj) => {
      this.cacheObj.disconnect(err => {
        if (err) return rj(err);
        return rs();
      });
    });
  }
  kafkaConnect() {
    return new Promise((rs, rj) => {
      this.cacheObj.connect((err) => {
        if (err) return rj(err);
        return rs();
      });
    });
  }
  sleep(millisecond = 300) {
    return new Promise(rs => {
      setTimeout(() => {
        rs();
      }, millisecond);
    });
  }
  static randomID(len = 17) {
    const str = '1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
    const slen = str.length;
    const r = [];
    for (let i = 0; i < len; i++) {
      r.push(str[Math.floor(Math.random() * slen)]);
    }
    return r.join('');
  }
}

module.exports = Producer;
// export default Producer;
