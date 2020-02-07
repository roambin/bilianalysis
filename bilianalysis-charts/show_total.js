var http =require('http');
var MongoClient = require('mongodb').MongoClient;
var url='mongodb://root:123@172.19.240.108:27017/admin?w=majority';
var koa=require('koa');
var app=new koa();
var Router = require('koa-router');
var router=new Router();
var views=require('koa-views');
app.use(views('views_streaming',{
    extension:'ejs'
}));
const static = require('koa-static');
app.use(static(__dirname+'/views_streaming'));
app.use(static(__dirname+'/lib'));
router.get('/',ctx => ctx.render('show_streaming'));
app.use(router.routes());
app.use(router.allowedMethods());
app.listen(3009);


var tag_view =[[], []];
var tag_hot =[[], []];
var time_view =[[], []];
router.post('/view/data', async (ctx) => {
    flushView();
    ctx.body = tag_view;
});
router.post('/hot/data', async (ctx) => {
    flushHot();
    ctx.body = tag_hot;
});
router.post('/time/data', async (ctx) => {
    flushTime();
    ctx.body = time_view;
});

function flushView() {
    MongoClient.connect(url,   (err,client)=>{
        if (err){
            console.log(err);
            return;
        }
        let database=client.db('total');
        let mysort = { view: -1 };
        let result=database.collection('tag_view').find().sort(mysort);
        result.toArray((err,docs)=>{
                if(docs !== undefined){
                    for (let i = 0; i < 30 && i < docs.length; i++) {
                        tag_view[0][i] = (docs[i].tag).toString();
                        tag_view[1][i] = (docs[i].view);
                    }
                }
                console.log(docs);
            }
        );
    })
}
function flushHot() {
    MongoClient.connect(url,   (err,client)=>{
        if (err){
            console.log(err);
            return;
        }
        let database=client.db('total');
        let mysort = { hot: -1 };
        let result=database.collection('tag_hot').find().sort(mysort);
        result.toArray((err,docs)=>{
                if(docs !== undefined){
                    for (let i = 0; i < 30 && i < docs.length; i++) {
                        tag_hot[0][i] = (docs[i].tag).toString();
                        tag_hot[1][i] = (docs[i].hot);
                    }
                }
                console.log(docs);
            }
        );
    })
}
function flushTime() {
    MongoClient.connect(url,   (err,client)=>{
        if (err){
            console.log(err);
            return;
        }
        let database=client.db('total');
        let mysort = { time: 1 };
        let result=database.collection('time_view').find().sort(mysort);
        result.toArray((err,docs)=>{
                if(docs !== undefined){
                    for (let i = docs.length < 30 ? 0 : docs.length - 30; i < docs.length; i++) {
                        time_view[0][i] = (docs[i].time).toString();
                        time_view[1][i] = (docs[i].view);
                    }
                }
                console.log(docs);
            }
        );
    })
}
