// 指定图表的配置项和数据
function setChartView(tag, data, myChart) {
    let option = {
        title: {
            text: '哔哩哔哩最多观看分类'
        },
        tooltip: {},
        legend: {
            data:['观看数'],
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
                        {type : 'max', name: '观看最多'},
                        //{type : 'min', name: '观看最少'}
                    ]
                },
                name: '观看数',
                type: 'bar',
                data: data,
                color:'#ff7da1'
            }]
    };
    myChart.setOption(option);
}

function flushView(myChart) {
    getDataView(myChart);
}

function getDataView(myChart) {
    $.ajax({
        type:'post',
        url:'http://localhost:3009/view/data',
        data: {

        },
        success:function(data){
            $("#stopped").slideUp();
            setChartView(data[0], data[1],myChart)
        },
        error:function(){
            $("#stopped").slideDown();
        }
    })
}