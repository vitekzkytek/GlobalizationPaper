mapObj =  {
    sizes: {
        width:1200,
        height:600,
        ddlwidth:400
    },
    default: {field_code:'All',method_code:'euclid',period:2017},
    current: {field_code:'All',method_code:'euclid',period:2017}
};

let map_svg,path,mapTooltip,gradient;
let mapColors = ['#4D0D01','#650B03','#7B0607','#910A16','#A70E28','#BB133E','#E95959','#F78585','#FFB4B2'];//'#D52E47',

function generateMap(selector) {
    measureMap();

    generateMapControls(selector + ' #mapCont');

    generateMapStatics(selector + ' #mapCont')
}
function measureMap() {
    mapObj.sizes.width = Math.max(sizes.chart.width,$(window).width()*0.6) ;
    mapObj.sizes.height = Math.max(sizes.chart.height,(screen.height - sizes.header.height) *0.6);
}
function generateMapControls(selector) {
    $(selector).append($('<div />',{id:'controls'})); //,style:`width:${mapObj.sizes.width}px`

    generateMapDDL(selector + ' #controls','fields');
    // generateMapDDL(selector + ' #controls','periods');
    generateMapDDL(selector + ' #controls','methods');
    generateSlider(selector + ' #controls', 'periods');
    generateMapTitle(selector + ' #controls');
}

function generateMapTitle(selector){
    $(selector).append($('<div />', {id:'mapTitle'}));

    $(selector + ' #mapTitle').append($('<h4 />'));

    updateMapTitle(selector + ' #mapTitle h4')
}

function updateMapTitle(selector) {
    let fields = $('#ddl_map_fields select').select2('data');
    let period = $('#periodSlider').slider('option','value');
    let methods = $('#ddl_map_methods select').select2('data');

    var s = "Globalization of science: ".concat(fields[0].text, " (").concat(period, "; ").concat(methods[0].text, ")");

    let h4 = $(selector);
    h4.empty();
    h4.html(s);
}

function generateSlider(selector, purpose) {

        $(selector).append($('<div />', {id:'periodSlider'}));
        div = $('#periodSlider');
        div.append($('<div />', {id:'custom-handle',class:'ui-slider-handle'}));
        var handle = $( "#custom-handle" );

        div.slider({
          create: function() {
            handle.text( 'Year: ' + $( this ).slider( "value" ) );
          },
          slide: function( event, ui ) {
                current = ui.value;
                if (current > 2017) {
                    current = 2017;
                    return false;
                }
            handle.text( 'Year: ' + ui.value );
          },
            min:2005,max:2019,
            value:2017,
            change:changeMap,
            start: function(event,ui) {
              handle.toggleClass('slided');
            },
            stop: function(event,ui) {
                handle.toggleClass('slided');
            }
        });

      }


function generateMapDDL(selector,purpose) {
    let ddlid = 'ddl_map_' + purpose;
    var div = $('<div />',{id:ddlid,class:'mapddl'}).appendTo($(selector));

    var ddlel = $('<select />').appendTo(div);
    var ddldata = purpose === 'periods' ? Array.from({
        length: 13
    }, function (x, i) {
        return i + 2005;
    }) : controllers[purpose].results;

    ddlel.select2({
        data: ddldata,
        width: mapObj.sizes.ddlwidth
    });
    ddlel.on('change', changeMap);
}

function mapcolor(val) { //https://hihayk.github.io/scale/#5/5/62/87/-25/-33/2/19/bb133e/187/19/62

    return mapColors[Math.floor((val)*10)]
}

var color = d3.scaleLinear()
    .domain([0,.25, .5,.75, 1])
    .range(["#650B03",'#650B03', "#BB133E",'#EFCCC4', "#FBF4F1"]);
// .range(["#650B03",'#650B03', "#BB133E",'#F78585', "#FFB4B2"]);


function generateMapStatics(selector) {
    $(selector).append($('<div />', {id:'mapLegend'}));



    var projection = d3.geoNaturalEarth1()
        .scale(mapObj.sizes.width / 1.5 / Math.PI)
        .translate([mapObj.sizes.width / 2,  mapObj.sizes.width / 3.5]);
    path = d3.geoPath()
        .projection(projection);
    $('#tooltip').remove();
    mapTooltip = d3.select('body')
        .append('div')
        .attr('id','mapTooltip')
        .style('opacity',0);

    map_svg = d3.select(selector + ' #mapLegend')
        .append('svg')
        .attr('width', mapObj.sizes.width)
        .attr('height', mapObj.sizes.height)
        .attr('id','mapSvg');

    drawMapLegend(selector + ' #mapLegend');

    updateMap();
}


function changeMap() {
    mapObj.current.field_code = $('#ddl_map_fields select').val();
    mapObj.current.method_code = $('#ddl_map_methods select').val();
    mapObj.current.period = $('#periodSlider').slider( 'value' );

    updateMap();

}

function drawMapLegend(selector) {
    function makeArr(startValue, steps, step) {
        var arr = [];
        var currValue = startValue;
        for (var i = 0; i < steps; i++) {
            arr.push(currValue + (step * i));
        }
        return arr;
    }
        let colorVals = makeArr(1,51,-0.02);
        var colorlist = colorVals.map(function (x) {
            return color(x);
        });
    $(selector).append($('<div />',{id:'justLegend'}));

        let rwidth = 20;

        let legHeight = mapObj.sizes.height*.5;
        let rheight = legHeight/colorlist.length;

        let leg_svg = d3.select(selector + ' #justLegend')
            .append('svg')
            .attr('id','mapLegendSvg')
            .attr('width',rwidth +30)
            .attr('height', legHeight + 20 );

        let gs = leg_svg.selectAll('g')
            .data(colorlist)
            .enter()
            .append('g')
            .attr('transform',function(d,i) {
                return 'translate(30,' + (10 + i*rheight) +')'
            });
        gs.append('rect')
            .attr('fill',function(d) {return d})
            .attr('stroke',function(d) {return d})
            .attr('width',50)
            .attr('height',rheight);

        gs.append('text')
            .attr('transform','translate(-5,10)')
            .text(function (d,i) {
                let val = Math.round(colorVals[i]*100);
                if (val % 20 === 0) {
                    return val/100
                }
            })
}

function updateMap() {
    updateMapTitle(' #mapTitle h4');
    function getMapData(method_code,field_code,period,callback) {
        prom = $.post('/map',{'field_code':field_code,period:period,method_code:method_code});
        prom.then(function (mapdata) {
            return callback(null, mapdata);
        });    }

    //var cntrs = svg.append('g').attr('id','countries');
    d3.queue()
        .defer(getMapData,mapObj.current.method_code,mapObj.current.field_code,mapObj.current.period)
        .defer(d3.json,'javascripts/world-110m.geojson')
        .await(function (error, mapdata,geodata) {
            let ndf = [];

            for (i in geodata.features) {
                let id = geodata.features[i].id;

                var el = mapdata.rows.find(function (el) {
                    return el.country_code === id;
                });

                if (typeof el !== 'undefined') {
                    geodata.features[i].mapvalue = el
                } else {
                    ndf.push(id)
                }
            }

            if (ndf.length > 0) {console.log(ndf)}

            map_svg.empty();
            map_svg.append("g")
                .attr("id", "countries")
                // .attr('transform','translate(-140,-140)')
                .selectAll("path")
                .data(geodata.features)
                .enter().append("path")
                .attr("id", function (d){
                    return d.id;
                })
                .attr('fill',function(d) {
                    if (typeof d.mapvalue !== 'undefined') {
                        return color(d.mapvalue.value)
                    } else {
                        return '#d1d4d3'
                    }
                })
                .attr("d", path)
                .on('mouseover', function(d) {
                    if (typeof d.mapvalue !== 'undefined') {
                        mapTooltip
                            .transition()
                            .duration(200)
                            .style('opacity', 0.9);
                        $(this).toggleClass('hoveredPath');
                        mapTooltip.html(d.mapvalue.name + ': ' + Math.round(d.mapvalue.value * 100) / 100)
                    }
                })
                .on('mouseout',function(d) {
                    if (typeof d.mapvalue !== 'undefined') {
                        mapTooltip.transition()
                            .duration(200)
                            .style('opacity', 0);
                        $(this).toggleClass('hoveredPath');
                    }
                })
                .on('mousemove',function(d) {
                    mapTooltip
                        .style('left',(d3.event.pageX) + 'px')
                        .style('top',(d3.event.pageY - 28) + 'px');
                });

        });

}