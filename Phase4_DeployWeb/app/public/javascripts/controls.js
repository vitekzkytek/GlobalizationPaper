ddlconfig = {
    fields: {
        placeholder:{
            en:'Select discipline ... ',
            cs:'Vyberte obor ... '
        },
        width:'90%',
        multiple:false,
        defaultVals:["All"],
        selectedVals:["All"],
        allowClear:true,
        maximumSelectionLength:10
    },
    countries: {
        placeholder:{
            en:'Select country ... ',
            cs:'Vyberte zemi ... '
        },
        width:'90%',
        multiple:true,
        defaultVals:["AUS", "EGY", "DEU", "IDN", "ITA", "MEX", "NGA", "POL", "RUS"],
        selectedVals:["AUS", "EGY", "DEU", "IDN", "ITA", "MEX", "NGA", "POL", "RUS"],
        allowClear:true,
        maximumSelectionLength:10
    },
    methods: {
        placeholder:{
            en:'Select indicator ... ',
            cs:'Vyberte metodu ... '
        },
        width:'90%',
        multiple:false,
        defaultVals:['euclid'],
        selectedVals:['euclid'],
        allowClear:true,
        maximumSelectionLength:6
    }
};



function generateControls(selector) {
    //controls = $('<div />', {id:'controls'}).appendTo(el);
    let st = 'width:' + (sizes.chart.width + sizes.legend.width) + ' px;left:'+(sizes.chart.left);
    $(selector).append($('<div />', {id: 'controls',style:st}));
    generateDDLs(selector + ' #controls')
}

function generateDDLs(selector) {
    $("<style type='text/css'> .select2-results { max-height; " + sizes.chart.height * 0.7 + "px;} </style>").appendTo("head");

    generateDDL(selector,'countries');
    generateDDL(selector,'fields');
    generateDDL(selector,'methods');
}

function generateDDL(selector,purpose) {
        ddlid = 'ddl_' + purpose;


    var svgcode = "<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" x=\"0px\" y=\"0px\" viewBox=\"0 0 512 512\" style=\"enable-background:new 0 0 512 512;\" xml:space=\"preserve\" width=\"".concat(sizes.switchCompare.width, "\" height=\"").concat(sizes.switchCompare.height, "\">\n            <g>\n               <path d=\"M512,119V0H392v40h53.716L262.5,223.216L122.837,83.553L40,170.28V0H0v512h512v-40H40V345.991l77.235-81.222l80.14,80.14 \n\t\t\tl65.125-65.125l104.5,104.5l105-105V328h40V209H392v40h53.716L367,327.716L290.784,251.5L472,70.284V119H512z M197.375,288.341\n\t\t\tl-80.86-80.86L40,287.944v-59.753l83.494-87.414L234.216,251.5L197.375,288.341z\"/>\n            </g>\n        </svg>");
        var div = $('<div />',{id:ddlid,class:'column'}).appendTo($(selector));
        switcherclass =  (ddlconfig[purpose].multiple) ? 'switchcompare multiswitcher ' :'switchcompare';

        if (purpose == 'methods') {
            div.append("<svg width=\"".concat(sizes.switchCompare.width, "\" height=\"").concat(sizes.switchCompare.height, "\" />"));        }
        else {
            div.append('<a class="' + switcherclass +'" onclick="switchMultiSelect(\'' + purpose + '\')">'+ svgcode +'</a>');
        }
        var ddlel = $('<select />').appendTo(div);
            let ddldata = controllers[purpose];
            ddlel.select2({
                placeholder: ddlconfig[purpose].placeholder[lang],
                allowClear: ddlconfig[purpose].multiple,
                data: ddldata.results,
                width: sizes.header.width - sizes.switchCompare.width - 50,
                multiple: ddlconfig[purpose].multiple,
                maximumSelectionLength: ddlconfig[purpose].maximumSelectionLength
            });


    ddlel.val(ddlconfig[purpose].selectedVals).trigger('change',[false]);

    ddlel.on('change',changeEvent);//,data=[true]);
}

function changeEvent(event) {

    var update = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;

    var purposes = Object.keys(ddlconfig);
    for (i in purposes) {
        var purpose = purposes[i];
        var ddl = $('#ddl_' + purpose + ' select');
        var selectedVals = ddl.val();
        if (Array.isArray(selectedVals)) {
            ddlconfig[purpose].selectedVals = selectedVals;
        } else {
            ddlconfig[purpose].selectedVals = [selectedVals];
        }

        ddl.val(ddlconfig[purpose].selectedVals)
    }
    if(update) {
        updLineChart();
    }
}

function switchMultiSelect(purpose) {
    var updateChart = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;


    var purposes = Object.keys(ddlconfig);
    for (i in purposes) {
        ddlconfig[purposes[i]].multiple = purposes[i] === purpose;
        if (purposes[i] !== purpose) {
            ddlconfig[purposes[i]].selectedVals = [ddlconfig[purposes[i]].selectedVals[0]]
        }
    }
    $('#app #controls').empty();
    generateDDLs('#app #controls');
    if (updateChart) {
        updLineChart();
    }
}

function displayMethod(method) {
    method_obj = controllers.methods.results.find(function (x) {
        return x.id === method;
    });

    $('.switchActive').removeClass('switchActive');
    $('#sw'+method).addClass('switchActive');
    $('#methodName').text(method_obj.full_name);
    $('#methodDescription').html(method_obj.description);
    MathJax.Hub.Queue(["Typeset",MathJax.Hub,"methodDescription"]);
    // let form = (method != 'weightGini')? '$' + method_obj.formula +'$' : '$' + method_obj.formula +' $\n$' + ' g_{gini} = \\sum_{i=1}^{n} v_i *w_{i-1} - \\sum_{i=0}^{n-1} v_i *w_{i+1}$';
    // $('#methodFormula').html(form);
    // MathJax.Hub.Queue(["Typeset",MathJax.Hub,"methodFormula"]);
    // $('#methodDesc').text(method_obj.short_desc);
    // $('#methodInput').text(method_obj.input);
    // MathJax.Hub.Queue(["Typeset",MathJax.Hub,"methodInput"]);
    // $('#methodSource').html(method_obj.source);

}