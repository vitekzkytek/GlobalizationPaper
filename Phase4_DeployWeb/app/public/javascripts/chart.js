let sizes;

responses = {};

let currentData = {};

var lc_x,lc_y,lc_line,lc_svg,lc_g,lc_legend,parseTime,lc_tooltip; //Line chart D3 constants

var lc_color = ["#3366cc", "#dc3912", "#ff9900", "#109618", "#990099", '#A0522D','#84cff4', '#CCCC00','#000000',"#22aa99", "#dd4477", "#66aa00", "#b82e2e", "#316395",
                "#994499", "#aaaa11", "#6633cc", "#e67300", "#8b0707", "#651067", "#329262", "#5574a6", "#3b3eac"];

function generateCharts(selector) {
    $(selector).append($('<div />', {
        id: 'chartCont',
        style: "width:".concat(sizes.chart.width + sizes.legend.width, "px;top:").concat(sizes.header.height, ";left:").concat(sizes.chart.left)
    }));

    genTitle(selector + ' #chartCont');
    genLineChart(selector + ' #chartCont');
}


function getLineColor(rank,faded) {
    if (faded) {
        return '#d1d4d3';
    } else {
        return lc_color[rank-1];
    }
}

function setSizes() {
    var totalWidth = Math.min(Math.max($(window).width() * 0.6,800),1000);
    var totalHeight = (window.innerHeight * 0.95);
    var legendWidth = 200;
    var footHeight = 50;
    var headHeight = 140;
    var titleHeight = (window.innerHeight * 0.05 + 12) * 1.36 + 10; //
    var boxwidth = 550;
    return {
        header:{
            width: totalWidth,
            height: headHeight
        },
        switchCompare: {
            width:40,height:30
        },
        title: {
            width: totalWidth,
            height: titleHeight
        },
        chart: {
            width: totalWidth - legendWidth,
            height: totalHeight - footHeight - headHeight - titleHeight,
            margin: {top:20,right:20,bottom:30,left:60},
            left:Math.max(($(window).width() - totalWidth - boxwidth) / 2,10)
        },
        foot: {
            width: totalWidth,
            height: footHeight
        },
        legend:{width:legendWidth},
        boxwidth:boxwidth
    }
}
function genLineChart(selector) {
    $(selector).append($('<div />', {id:'LineChartCont'}));
    $('div.tooltip').remove();


    lc_tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);


    lc_svg = d3.select(selector + ' #LineChartCont')
        .append('svg')
        .attr('id','svgLineChart')
        .attr('width',sizes.chart.width + sizes.legend.width + 'px')
        .attr('height',sizes.chart.height + 'px');
    lc_g = lc_svg.append("g")
        .attr("transform", "translate(" + sizes.chart.margin.left + "," + sizes.chart.margin.top + ")");

    var height = sizes.chart.height - sizes.chart.margin.top - sizes.chart.margin.bottom;

    var width = sizes.chart.width - sizes.chart.margin.left - sizes.chart.margin.right;

    parseTime = d3.timeParse('%Y');


    lc_x = d3.scaleTime()
        .rangeRound([0,width])
        .domain([parseTime(2004),parseTime(2018)]);

    lc_y = d3.scaleLinear()
        .rangeRound([height,0])
        .domain([0,1]);


    lc_line = d3.line()
        .defined(function (d) {return d; })
        .x(function(d) {
            return lc_x(parseTime(d.period));
        })
        .y(function(d) {
            return lc_y(d.value);
        });

    genFooter(selector);

    lc_g.append('g')
        .attr('id','x-axis')
        .attr('transform','translate(0,' + height + ')')
        .call(d3.axisBottom(lc_x));
        //.select('.domain').remove() - wtf is doing this?

    $('#x-axis g.tick').first().text('');
    $('#x-axis g.tick').last().text('');

    lc_g.append('g')
        .attr('id','y-axis')
        .call(d3.axisLeft(lc_y).ticks(6));


    let yxg = lc_g.append('g')
        .attr("transform", "rotate(-90)")

        .attr('style','font-weight:bold;')
        .style("text-anchor", "middle");

    yxg.append("text")
        .attr('id','globlabel')
        .attr("y", 0 - sizes.chart.margin.left)
        .attr("x",0 - (sizes.chart.height / 2))
        .attr("dy", "1em")
        .text("Globalization index");


    yxg.append('image')
        .attr("y", -15 - sizes.chart.margin.left)
        .attr("x",d3.select('#globlabel')._groups[0][0].getBBox().x + $('#globlabel').height()+5)
        .attr('xlink:href','img/g_z_cdy.JPG')
        // .attr('x',-40)
        // .attr('y',lc_y(.5))
        .attr('width',60)
        .attr('height',50);


    lc_g.append('g').attr('id','lines');

    lc_legend = lc_g.append('g')
        .attr('id','legend')
        .attr('transform','translate(' +  (sizes.chart.width - sizes.chart.margin.left - sizes.chart.margin.right) +  ', '+ lc_y(.75) + ')');

    updLineChart()
}

function updLineChart() {
    var reqsList = genReqsList();

    let reqs = reqsList[0];
    let mult = reqsList[1];

    var j = 0;
    var rank = 0;

    updTitle('#appCont #lineChartTitle h4');
    updFooter('#appCont #LineChartFooter');
    let reqproms = [];
    for (var i in reqs) {
        reqproms.push({promise:getLinePromise(reqs[i],i),request:reqs[i],multikey:mult});
    }

    var promises = reqproms.map(function (a) {
        return a.promise;
    });

    $.when.apply($,promises).then(function() {
        currentData = {};
        lc_g.select('#lines').selectAll("*").remove();
        lc_g.select('#legend').selectAll("*").remove();

        let anyFaded = reqproms.some(function(el) {return el.request.faded});
        let args = (Array.isArray(arguments[0])) ? Array.from(arguments) : [Array.from(arguments)];


        for (var i in args) {
            var ix = (anyFaded) ? i : parseInt(i)+1;

            DrawLine(args[i][0], ix, reqproms[i].request,reqproms[i].multikey);

            // var argId = args[i][0].data[reqproms[i].multikey][0]['id'];
            var argId = reqproms[i].request[reqproms[i].multikey];
            currentData[argId] = args[i][0].rows;
        }
        DrawSortedLegend(args,reqproms,reqproms[0].multikey,anyFaded)
    });


}

function DrawSortedLegend(args,reqproms,multikey,anyFaded) {

    function sortLegData(a,b) {
        if ((typeof a.index == 'object') && (typeof b.index == 'object')) {
            return b.index.value - a.index.value;
        }
        else {
            return 1;
        }
    }
    let ArgsArr = Array.from(args);
    var legData = ArgsArr.map(function (a, i) {
        var xrank = anyFaded ? ArgsArr.indexOf(a) : ArgsArr.indexOf(a) + 1;
        var xid = reqproms[i].request[multikey];
        return {
            // index:a[0].data.interindexes.slice(-1)[0],
            index: a[0].rows.sort(function (a, b) {
                return a.period > b.period ? 1 : -1;
            }).slice(-1)[0],
            // text:a[0].data[multikey][0].name,
            text: controllers[multikey].results.find(function (group) {
                return group.children.some(function (item) {
                    return item.id === xid;
                });
            }).children.find(function (x) {
                return x.id === xid;
            }).legname,
            rank: xrank,
            faded: reqproms[ArgsArr.indexOf(a)].request.faded
        };
    });
    legData = legData.sort(sortLegData);
//function(a,b) {return b.index.value - a.index.value}
    var faded = legData.filter(function (a) {
        return a.faded;
    });
    var colored = legData.filter(function (a) {
        return !a.faded;
    });
    let maingroup = lc_legend
        .selectAll('g')
        .data(['faded','colored'])
        .enter()
        .append('g')
        .attr('id',function (d) { return d; });

    let gColor = maingroup.select('#colored')
        .data(colored)
        .enter()
        .append('g')
        .attr('id',function(d) {return d.text})
        .attr('transform',function(d) { return 'translate(0,' + legData.indexOf(d) * 25 + ')' })
        .attr('class','legItem');
    gColor.append('circle')
        .attr('r', 7)
        .attr('fill', function(d) {return getLineColor(d.rank,false)});
    gColor.append('text')
        .attr('transform','translate(12,7)')
        .text(function(d) {return d.text});

    let gFaded = maingroup.select('#faded')
        .data(faded)
        .enter()
        .append('g')
        .attr('id',function(d) {return d.text})
        .attr('transform',function(d) { return 'translate(0,' + legData.indexOf(d) * 25 + ')' })
        .attr('class','legItem');
    gFaded.append('line')
        .attr('x1',0)
        .attr('x2',20)
        .attr('y1',0)
        .attr('y2',0)
        .attr('stroke',function(d) {return getLineColor(d.rank,true)})
        .attr('stroke-width',1.5)
        .attr('stroke-dasharray',8);

    gFaded.append('text')
        .attr('transform','translate(12,7)')
        .text(function(d) {return d.text;});

    let mx = Math.max(legData[0].index.value,0.3);


    let maxim = (lc_y(0) - lc_y(mx) > $('#legend').height()) ? lc_y(mx) : lc_y(1);

    lc_legend
        .attr('transform','translate(' +  (sizes.chart.width - sizes.chart.margin.left - sizes.chart.margin.right) +  ', '+  maxim + ')');
}

function getLinePromise(req, rank) {
    var ajaxobj = retrieveInterindex(req);
    //$.post('/map',{'field_code':field_code,period:period,method_code:method_code});
    return $.post(ajaxobj.url, ajaxobj.data, function (result, status, response) {});
}
function DrawLine(result,rank,request,multikey) {
    //yrs = Array.from(new Array(13), (x,i) => i + 2005);
    yrs = Array.from(new Array(13), function (x, i) {
        return i + 2005;
    });

    function filterResults (yr) {
            // el = result.data.interindexes.find(x=> x.period === yr);
            el = result.rows.find(function (x) {
                return x.period === yr;
            });
            if(typeof el === 'undefined') {
                return null
            }
            else {
                return {period:yr,value:el.value}
            }
    }
    let data = yrs.map(filterResults);

    lines = lc_g.select('#lines');
    lines.append('path')
        // .attr('id',result.data[multikey][0]['id'])
        .attr('id',request[multikey])
        .datum(data)
        .attr("fill", "none")
        //.attr("stroke", function() {if (faded) {return '#d1d4d3'} else {return lc_color[rank]}})
        .attr("stroke", function() {
            return getLineColor(rank,request.faded)
        })
        .attr("stroke-linejoin", "round")
        .attr("stroke-linecap", "round")
        .attr("stroke-width", function() {if (request.faded) {return 1.5} else {return 1.5}})
        .attr("stroke-dasharray", function() {if (request.faded) {return 8} else {return 0}})
        .attr("d", lc_line)
        .on('mouseover', function (d) {
            let xid = $(this).attr('id');
            var legname = controllers[multikey].results.find(function (group) {
                return group.children.some(function (item) {
                    return item.id === xid;
                });
            }).children.find(function (x) {
                return x.id === xid;
            }).legname;

            lc_tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            lc_tooltip.html(legname)
                .style('color',d3.select(this).attr('stroke'))
                .style("left", (d3.event.pageX) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
        })
        .on('mouseout', function(d) {
            lc_tooltip.transition()
                .duration(500)
                .style("opacity", 0);

        });
    if (!request.faded) {
        lines.append('g')
            .attr('class','circles')
            // .attr('id',result.data[multikey][0]['id'])
            .attr('id',request[multikey])

            .selectAll('circle')
            .data(data.filter(function(d) { return d; }))
            .enter()
            .append('circle')
            .attr('r', 3)
            .attr('stroke', getLineColor(rank,false))
            .attr('stroke-width', 2)
            .attr('fill', getLineColor(rank,false))
            .attr('cx', function (d) {
                return lc_x(parseTime(d.period))
            })
            .attr('cy', function (d) {
                return lc_y(d.value)
            })
            .on("mouseover", function (d) {
                lc_tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                lc_tooltip.html(Math.round(d.value * 100) / 100)
                    .style('color',d3.select('#'+$(this).parent().attr('id') + ' circle').attr('fill'))
                    .style("left", (d3.event.pageX) + "px")
                    .style("top", (d3.event.pageY - 28) + "px");
            })
            .on("mouseout", function (d) {
                lc_tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            });
    }

}

function genReqsList() {
    var purposes = Object.keys(ddlconfig);
    var multikey = Object.keys(ddlconfig).find(function (key) {
        return ddlconfig[key].multiple === true;
    });
    var result = [];

    if (multikey === 'countries') {
        if (!ddlconfig.countries.selectedVals.includes('_AV')) {
            result.push({
                countries: '_AV',
                fields: ddlconfig.fields.selectedVals[0],
                methods: ddlconfig.methods.selectedVals[0],
                faded: true
            });
        }
    }
    if (multikey === 'fields') {
        if(!ddlconfig.fields.selectedVals.includes('All')) {
            result.push({
                countries: ddlconfig.countries.selectedVals[0],
                fields: 'All',
                methods: ddlconfig.methods.selectedVals[0],
                faded: true
            });
        }
    }



    for (i = 0; i < ddlconfig[multikey].selectedVals.length; i++) {
        var obj = {};
        for (p in purposes) {
            // obj['faded'] = purposes[p] === 'countries';

            obj[purposes[p]] = (purposes[p] === multikey) ? ddlconfig[purposes[p]].selectedVals[i] : ddlconfig[purposes[p]].selectedVals[0];
        }
        obj['faded'] = false;
        result.push(obj)
    }
    return [result,multikey];
}

function updTitle(selector) {
    el = $(selector + ' #LineChartTitle');

    var multipurpose = Object.keys(ddlconfig).find(function (x) {
        return ddlconfig[x].multiple === true;
    });

    let s = '';

    let fields = $('#ddl_fields select').select2('data');
    let countries = $('#ddl_countries select').select2('data');
    let methods = $('#ddl_methods select').select2('data');

    switch (multipurpose) {
        case 'countries':
            s = "Globalization of science: ".concat(fields[0].text); // by ${methods[0].text}`;

            break;

        case 'fields':
            s = "Globalization of science: ".concat(countries[0].text); // by ${methods[0].text}`;

            break;

        case 'methods':
            s = "Globalization of science: ".concat(countries[0].text); // in ${fields[0].text}`;

            break;
    }
    $(selector).text(s)
}

function genTitle(selector) {
    $(selector).append($('<div />', {id:'LineChartTitle'}));
    $(selector + ' #lineChartTitle').append('<h4 />');

    updTitle(selector + ' #lineChartTitle h4');
    $(selector + ' #lineChartTitle h4').css('width', sizes.title.width + 'px');
}

function genFooter(selector) {
    $(selector).append($('<div />', {id:'LineChartFooter'}));
    updFooter(selector + ' #LineChartFooter');
}

function updFooter(selector) {
    $(selector).empty();

    s = 'Note: 0 and 1 refer to minimum and maximum of the indicator across all <a class="modalLink" onclick="showModal(\'modCountries\')">countries</a> ' +
        '(or <a class="modalLink" onclick="showModal(\'modCountryGroups\')">country groups</a>),' +
        ' <a class="modalLink" onclick="showModal(\'modDisciplines\')">disciplines</a> and years; ' +
        'based on ' + $('#ddl_methods select').select2('data')[0].text +
        '; Source: <a class="modalLink" onclick="showModal(\'modScopus\')">Scopus</a>.';
    $(selector).append('<p>'+ s +'</p>');


    MathJax.Hub.Queue(["Typeset",MathJax.Hub,"LineChartFooter"]);

}