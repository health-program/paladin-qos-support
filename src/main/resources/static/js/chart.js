/* 
 * dataType = [1:日处理数据,2:月处理数据,3:年处理数据]
 * 
 */
tonto.Heatmap = function(domId) {
    var dom = document.getElementById(domId),
        that = this;
    that.chart = echarts.init(dom);
}

tonto.Heatmap.prototype.updateData = function(options) {
    var that = this,
        defaultOption = {
            dataType: 1,
            dataNeedProcess: true,
            eventName: '概率',
            totalNumName: '样本数',
            eventNumName: '事件数'
        },
        ajax = options.data ? false : true;

    that.dataOptions = $.extend(defaultOption, options);

    if (ajax) {
        $.postAjax("/qos/analysis/data/processed", that.dataOptions, function(data) {
            that.processData(data);
            that.updateChart();
        });
    } else {
        that.processData(options.data);
        that.updateChart();
    }
}

tonto.Heatmap.prototype.processData = function(data) {
    var that = this,
        dataType = that.dataOptions.dataType;

    if (dataType == 1) {
        that.dataMap = new tonto.DayData(data, that, that.dataOptions.dateSelector);
    } else if (dataType == 2) {
        that.dataMap = new tonto.MonthData(data, that, that.dataOptions.dateSelector);
    } else if (dataType == 3) {
        that.dataMap = new tonto.YearData(data, that, that.dataOptions.dateSelector);
    }
}

tonto.Heatmap.prototype.updateChart = function(year, month, day) {
    var that = this,
        dataOptions = that.dataOptions,
        data = that.dataMap.getData(year, month, day),
        chartOptions = dataOptions.chartOptionsHandle;

    that.chartOptions = {
        tooltip: {
            trigger: 'item',
            formatter: function(params, ticket, callback) {
                var data = params.value[3];
                return data.year + '年' + data.month + '月' + data.day + '日<br>' +
                    dataOptions.eventNumName + ' : ' + data.eventNum + '<br>' +
                    dataOptions.totalNumName + ' : ' + data.totalNum + '<br>' +
                    dataOptions.eventName + ' : ' + params.value[2] + '%';
            },
            backgroundColor: 'rgba(245, 245, 245, 0.8)',
            borderWidth: 1,
            borderColor: '#ccc',
            padding: 10,
            textStyle: {
                color: '#000'
            }
        },
        animation: false,
        grid: {
            top: 35,
            right: 20,
            botton: 100,
            left: 110
        },
        xAxis: {
            type: 'category',
            data: data.xAxis,
            splitArea: {
                show: true
            }
        },
        yAxis: {
            type: 'category',
            data: data.yAxis,
            splitArea: {
                show: true
            }
        },
        visualMap: {
            type: 'piecewise',
            min: 0,
            max: 12,
            splitNumber: 4,
            dimension: 2,
            show: true,
            orient: 'horizontal',
            right: 30,
            top: 'top'
        },
        series: [{
            name: 'heatmapChart',
            type: 'heatmap',
            data: data.data,
            label: {
                normal: {
                    show: true,
                    formatter: function(v) {
                        return v.data[2].toFixed(0);
                    }
                }
            }
        }]
    };

    if (typeof chartOptions === 'function') {
        chartOptions(that.chartOptions, that);
    }

    that.chart.setOption(that.chartOptions, true);
}

tonto.DayData = function(data, chart, dateSelector) {
    var that = this,
        units = [],
        calendarDataMap = {};

    that.chart = chart;

    var getRate = function(row) {
        var e = row.eventNum,
            t = row.totalNum;
        if (t == 0) return 0;
        var x = e * 100 / t;
        if (x < 0.005) {
            return 0;
        }
        return x.toFixed(2) * 1;
    }

    var getMonthData = function(unitPoints, s, e) {
        var monthData = [];
        for (var k = 0; k < unitPoints.length; k++) {
            var ps = unitPoints[k].points;
            var t = 0;
            for (var l = s; l < e; l++) {
                monthData.push([t, k, getRate(ps[l]), ps[l]]);
                t++;
            }
        }
        return monthData;
    }

    var unitPoints = data.unitPoints;
    var firstDay, lastDay, firstMonth, lastMonth, firstYear, lastYear;

    if (unitPoints && unitPoints.length > 0) {
        for (var i = 0; i < unitPoints.length; i++) {
            units.push(unitPoints[i].unitName);
        }

        var unitPoint = unitPoints[0];
        var points = unitPoints[0].points;

        if (points && points.length > 0) {

            var year = points[0].year,
                month = points[0].month,
                day = points[0].day,
                size = 0,
                s = 0,
                j = 0;

            firstDay = day;
            firstMonth = month;
            firstYear = year;

            for (; j < points.length; j++) {
                var p = points[j];
                size++;
                var y = p.year,
                    m = p.month;
                if (month != m) {
                    var yearDataMap = calendarDataMap[year];
                    if (!yearDataMap) {
                        yearDataMap = {};
                        calendarDataMap[year] = yearDataMap;
                    }

                    yearDataMap[month] = {
                        year: year,
                        month: month,
                        day: day,
                        size: size - 1,
                        points: getMonthData(unitPoints, s, j)
                    };

                    month = m;
                    year = y;
                    day = 1;
                    s = j;
                    size = 1;
                }
            }

            lastYear = year;
            lastMonth = month;
            lastDay = points[j - 1].day;

            var yearDataMap = calendarDataMap[lastYear];
            if (!yearDataMap) {
                yearDataMap = {};
                calendarDataMap[lastYear] = yearDataMap;
            }

            yearDataMap[lastMonth] = {
                year: lastYear,
                month: lastMonth,
                points: getMonthData(unitPoints, s, j)
            };
        }

        that.firstYear = firstYear;
        that.firstMonth = firstMonth;
        that.firstDay = firstDay;

        that.lastYear = lastYear;
        that.lastMonth = lastMonth;
        that.lastDay = lastDay;

        that.units = units;
        that.calendarDataMap = calendarDataMap;
    }

    if (dateSelector) {
    	var min  =that.firstYear + '-' + that.firstMonth + '-' + firstDay;
        laydate.render({
            elem: dateSelector,
            type: 'month',
            min: that.firstYear + '-' + that.firstMonth + '-' + firstDay,
            max: that.lastYear + '-' + that.lastMonth + '-' + lastDay,
            value: that.firstYear + '-' + that.firstMonth,
            done: function(value, date, endDate) {
                chart.updateChart(date.year, date.month);
            }
        });
    }
}

tonto.DayData.prototype.getData = function(year, month) {
    var that = this,
        year = year || that.firstYear,
        month = month || that.firstMonth;

    var data = that.calendarDataMap[year];
    if (data) {
        data = data[month];
    }

    if (data) {
        var xAxis = [];
        for (var s = data.day, i = 0; i < data.size; s++, i++) {
            xAxis.push(s);
        }
        return {
            xAxis: xAxis,
            yAxis: that.units,
            data: data.points
        };
    }

    return null;
}

tonto.DayData.prototype.getFirstData = function() {
    return this.getData(this.firstYear, this.firstMonth);
}

