const puppeteer = require('puppeteer-core');
const {executablePath} = require('puppeteer-core');
const sleep = ms => new Promise(res => setTimeout(res, ms));
const { execSync } = require('child_process');


var numbers = [];

//1.まずは余計なの外す
// ^(?!.*\([0-9]{4}\)).*$ → 消す
//2.最後に数字だけのこす
// \(|\).* → 消す

'use strict';
//モジュールの読み込み
const fs = require('fs');
const readline = require('readline');

//readstreamを作成
const rs = fs.createReadStream('H:\\desctop\\株攻略\\1-スクリーニング自動化プログラム\\input.csv');
//writestreamを作成
const ws = fs.createWriteStream('H:\\desctop\\株攻略\\1-スクリーニング自動化プログラム\\output.csv');

//インターフェースの設定
const rl = readline.createInterface({
//読み込みたいストリームの設定
  input: rs,
//書き出したいストリームの設定
  output: ws
});

//1行ずつ読み込む設定
rl.on('line', (lineString) => {
  //wsに一行ずつ書き込む
  console.log("A"+lineString);
  lineString = lineString.replace(/^(?!.*\([0-9]{4}\)).*$/, '');
  console.log("B"+lineString);
  lineString = lineString.replace(/\(/, '');
  lineString = lineString.replace(/\)/, '');
  lineString = lineString.replace(/\t.*/, '');
  if(lineString.length != 0){
   ws.write(lineString + ',"",TKY,,,,,,\n');
   numbers.push(lineString);
  }
});
rl.on('close', () => {
  console.log("END!");
});




