//var myChart = echarts.init(document.getElementById('main'));
// 指定图表的配置项和数据
function setChartHot(tag, data, myChart) {
    var option = {
        title: {
            text: '哔哩哔哩最热分类'
        },
        tooltip: {},
        legend: {
            data:['热度'],
        },
        xAxis: {
            type: 'category',
            data: tag,
            axisLabel:{
                interval:0,//0：全部显示，1：间隔为1显示对应类目，2：依次类推，（简单试一下就明白了，这样说是不是有点抽象）
                rotate:-20,//倾斜显示，-：顺时针旋转，+或不写：逆时针旋转
            }
        },
        yAxis: {},
        series: [

            {
                markPoint : {
                    data : [
                        {type : 'max', name: '最热'},
                        //{type : 'min', name: '最冷'}
                    ]
                },
                name: '热度',
                type: 'bar',
                data: data,
                color:'#ff7da1'
            }]
    };
    myChart.setOption(option);
}

// 使用刚指定的配置项和数据显示图表。

function flushHot(myChart) {
    getDataHot(myChart);
}

function getDataHot(myChart) {
    $.ajax({
        type:'post',
        url:'http://localhost:3009/hot/data',
        data: {
            age: 18
        },
        success:function(data){
            setChartHot(data[0], data[1],myChart)
        },
        error:function(){
            alert('closed');
        }
    })
}