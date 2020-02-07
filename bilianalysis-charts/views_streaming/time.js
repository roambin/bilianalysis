// 指定图表的配置项和数据
function setChartTime(time, data, myChart) {
    let option = {
        title: {
            text: '哔哩哔哩每日观看数'
        },
        tooltip: {},
        legend: {
            data:['观看数'],
        },
        xAxis: {
            type: 'category',
            data:time,
        },
        yAxis: {
            type: 'value'
        },
        series: [{
            name:'观看数',
            data: data,
            type: 'line',
            color:'#1c7ccb',
            markPoint : {
                data : [
                    {type : 'max', name: '观看最多'},
                    {type : 'min', name: '观看最少'}
                ]
            },
        }]
    };
    myChart.setOption(option);
}

function flushTime(myChart) {
    getDataTime(myChart);
}

function getDataTime(myChart) {
    $.ajax({
        type:'post',
        url:'http://localhost:3009/time/data',
        data: {

        },
        success:function(data){
            $("#stopped").slideUp();
            setChartTime(data[0], data[1],myChart)
        },
        error:function(){
            $("#stopped").slideDown();
        }
    })
}