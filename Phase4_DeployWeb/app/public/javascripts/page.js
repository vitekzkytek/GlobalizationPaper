var lang = 'en';
let weblink = 'http://www.globalizationofscience.com/';
let webtitle = 'Globalization of Science';



var waypoints;
if (!Array.prototype.includes) {
    Object.defineProperty(Array.prototype, "includes", {
        enumerable: false,
        value: function(obj) {
            var newArr = this.filter(function(el) {
                return el == obj;
            });
            return newArr.length > 0;
        }
    });
}
// https://tc39.github.io/ecma262/#sec-array.prototype.find
if (!Array.prototype.find) {
    Object.defineProperty(Array.prototype, 'find', {
        value: function(predicate) {
            // 1. Let O be ? ToObject(this value).
            if (this == null) {
                throw new TypeError('"this" is null or not defined');
            }

            var o = Object(this);

            // 2. Let len be ? ToLength(? Get(O, "length")).
            var len = o.length >>> 0;

            // 3. If IsCallable(predicate) is false, throw a TypeError exception.
            if (typeof predicate !== 'function') {
                throw new TypeError('predicate must be a function');
            }

            // 4. If thisArg was supplied, let T be thisArg; else let T be undefined.
            var thisArg = arguments[1];

            // 5. Let k be 0.
            var k = 0;

            // 6. Repeat, while k < len
            while (k < len) {
                // a. Let Pk be ! ToString(k).
                // b. Let kValue be ? Get(O, Pk).
                // c. Let testResult be ToBoolean(? Call(predicate, T, « kValue, k, O »)).
                // d. If testResult is true, return kValue.
                var kValue = o[k];
                if (predicate.call(thisArg, kValue, k, o)) {
                    return kValue;
                }
                // e. Increase k by 1.
                k++;
            }

            // 7. Return undefined.
            return undefined;
        },
        configurable: true,
        writable: true
    });
}
// Production steps of ECMA-262, Edition 6, 22.1.2.1
if (!Array.from) {
    Array.from = (function () {
        var toStr = Object.prototype.toString;
        var isCallable = function (fn) {
            return typeof fn === 'function' || toStr.call(fn) === '[object Function]';
        };
        var toInteger = function (value) {
            var number = Number(value);
            if (isNaN(number)) { return 0; }
            if (number === 0 || !isFinite(number)) { return number; }
            return (number > 0 ? 1 : -1) * Math.floor(Math.abs(number));
        };
        var maxSafeInteger = Math.pow(2, 53) - 1;
        var toLength = function (value) {
            var len = toInteger(value);
            return Math.min(Math.max(len, 0), maxSafeInteger);
        };

        // The length property of the from method is 1.
        return function from(arrayLike/*, mapFn, thisArg */) {
            // 1. Let C be the this value.
            var C = this;

            // 2. Let items be ToObject(arrayLike).
            var items = Object(arrayLike);

            // 3. ReturnIfAbrupt(items).
            if (arrayLike == null) {
                throw new TypeError('Array.from requires an array-like object - not null or undefined');
            }

            // 4. If mapfn is undefined, then let mapping be false.
            var mapFn = arguments.length > 1 ? arguments[1] : void undefined;
            var T;
            if (typeof mapFn !== 'undefined') {
                // 5. else
                // 5. a If IsCallable(mapfn) is false, throw a TypeError exception.
                if (!isCallable(mapFn)) {
                    throw new TypeError('Array.from: when provided, the second argument must be a function');
                }

                // 5. b. If thisArg was supplied, let T be thisArg; else let T be undefined.
                if (arguments.length > 2) {
                    T = arguments[2];
                }
            }

            // 10. Let lenValue be Get(items, "length").
            // 11. Let len be ToLength(lenValue).
            var len = toLength(items.length);

            // 13. If IsConstructor(C) is true, then
            // 13. a. Let A be the result of calling the [[Construct]] internal method
            // of C with an argument list containing the single item len.
            // 14. a. Else, Let A be ArrayCreate(len).
            var A = isCallable(C) ? Object(new C(len)) : new Array(len);

            // 16. Let k be 0.
            var k = 0;
            // 17. Repeat, while k < len… (also steps a - h)
            var kValue;
            while (k < len) {
                kValue = items[k];
                if (mapFn) {
                    A[k] = typeof T === 'undefined' ? mapFn(kValue, k) : mapFn.call(T, kValue, k);
                } else {
                    A[k] = kValue;
                }
                k += 1;
            }
            // 18. Let putStatus be Put(A, "length", len, true).
            A.length = len;
            // 20. Return A.
            return A;
        };
    }());
}

function loadJS() {
    $('.urlField').val(weblink);
    if (detectIE()) {



        let warnDevs = ['Tablet', 'Smart-TV', 'Other non-Mobile', 'Other Mobile', 'Robot', 'App', 'Smartphone', 'Feature Phone'];
        if (warnDevs.includes(WURFL.form_factor)) {
            showModal('modSpecialDevice');
        }

        let banDevs = [];
        if (banDevs.includes(WURFL.form_factor)) {
            showModal('modRozliseni');
            $('#modalWrap').off('click');
        }
    }

    sizes = setSizes();

    waypoints = waypointing();

    // checkResolution();

    generateApp('#app');

    dragify();

    shareLinks();

    generateMap('#map');

}
function checkResolution() {
    w = $(window).width();

    if (w<999) {
        showModal('modRozliseni');
        $('#modalWrap').off('click');
    }
}

function showCopyLink() {
    showModal('modCopyLink')
}

function dragify() {

    function moveAll(evt) {
        $('.draggable').css('left',$(this).css('left'));
    }
    drags = $( ".draggable" );
    drags.draggable({
        handle: ".dragButton" ,
        stop: moveAll
    });
    let leftpos = ($(window).width() - sizes.header.width > sizes.boxwidth) ? sizes.header.width + sizes.chart.left: $(window).width() - sizes.boxwidth - 20;
    drags.css('left',leftpos);

}

function generateApp(selector) {
    $(selector).append($('<div />', {id:'appCont'}));

    generateControls(selector + ' #appCont');

    generateCharts(selector + ' #appCont');


}

function waypointing() {
    function activatefix(selector) {
        $('.fixactive').removeClass('fixactive');
        $(selector).addClass('fixactive');
    };

    function fixBox(selector,parent,target,toppos) {
        element = $(parent+ ' ' +selector).detach();
        $(target).append(element);
        $(target  + ' ' + selector).css({top:toppos,'box-shadow':'0 0 0 0'})
    }

    function floatBox(selector,parent,target) {
        element = $(parent + ' ' + selector).detach();
        $(target).append(element);
        $(element).css({top:'0px','box-shadow': '0px 0px 20px 4px #d1d4d3'})
    }

    function updChart(selCountries,selFields,selMethods) {
        $('#ddl_countries').val()
    }


    // fixing menu and adding shadow
    waypoints = $('#menu').waypoint(function(direction) {
        if(direction === 'down') {
            $('#everything').append($('<div class="stickyshadow"></div>'));
            $('#menu').addClass('sticky');
            $('#menu').removeClass('floaty');
            $('#menuempty').css('display','block')
        } else {
            $('#menu').addClass('floaty');
            $('#menu').removeClass('sticky');
            $('#menuempty').css('display','none')
            $('.stickyshadow').remove();
        }});

    waypoints = $('#context').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mContext').addClass('storyPast')
            } else {
                $('#mContext').removeClass('storyPast')
            }},offset:'17%'});

    waypoints = $('#empt-app').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mApp').addClass('storyPast')
            } else {
                $('#mApp').removeClass('storyPast')
            }},offset:'17%'});




    waypoints = $('#big7').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mCountries').addClass('storyPast')
            } else {
                $('#mCountries').removeClass('storyPast')
            }},offset:'17%'});

    waypoints = $('#advanced').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mDisciplines').addClass('storyPast')
            } else {
                $('#mDisciplines').removeClass('storyPast')
            }},offset:'17%'});


    waypoints = $('#map').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mMap').addClass('storyPast')
            } else {
                $('#mMap').removeClass('storyPast')
            }},offset:'17%'});



    waypoints = $('#context').waypoint(function(direction) {
        if (direction === 'down') {
            activatefix('#app')
        } else {
            activatefix('#intro')
        }});

    waypoints = $('#transition').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mEast').addClass('storyPast')
            } else {
                $('#mEast').removeClass('storyPast')
            }},offset:'17%'});


    waypoints = $('#china').waypoint(function(direction) {
            if(direction === 'down') {
                $('#mChina').addClass('storyPast')
            } else {
                $('#mChina').removeClass('storyPast')
            }
        },
        {offset:'17%'}
    );

    waypoints = $('#russia').waypoint(function(direction) {
            if(direction === 'down') {
                $('#mRussia').addClass('storyPast')
            } else {
                $('#mRussia').removeClass('storyPast')
            }
        },
        {offset:'17%'}
    );
    waypoints = $('#conclusion').waypoint({handler:function(direction) {
            if (direction === 'down') {
                $('#mConclusion').addClass('storyPast')
            } else {
                $('#mConclusion').removeClass('storyPast')
            }},offset:'17%'});

    waypoints = $('#broad').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(['_ADV','_TRA','_DEV']).trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);
                $('#ddl_fields select').val('All').trigger('change',[true]);
            } else {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(["AUS", "EGY", "DEU", "IDN", "ITA", "MEX", "NGA", "POL", "RUS"]).trigger('change',[false]);
                $('#ddl_fields select').val("All").trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);

                $('#ddl_countries select').trigger('change',[true]);
            }
        },        {offset:'60%'}
    );


    waypoints = $('#big7').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(["_EU", "BRA", "CHN", "IND", "JPN", "RUS", "USA"]).trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);
                $('#ddl_fields select').val('All').trigger('change',[true]);
            } else {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(['_ADV','_TRA','_DEV']).trigger('change',[false]);
                $('#ddl_fields select').val("All").trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);

                $('#ddl_countries select').trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );
    waypoints = $('#top5bottom5').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(['RUS','AZE','CUB','UKR','TJK','CHN','KAZ','BLR','UZB','ROU']).trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);
                $('#ddl_fields select').val("All").trigger('change',[true]);
            } else {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(["_EU", "BRA", "CHN", "IND", "JPN", "RUS", "USA"]).trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);
                $('#ddl_fields select').val('All').trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );


    waypoints = $('#advanced').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("_ADV").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            } else {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(['RUS','AZE','CUB','UKR','TJK','CHN','KAZ','BLR','UZB','ROU']).trigger('change',[false]);
                $('#ddl_methods select').val('euclid').trigger('change',[false]);
                $('#ddl_fields select').val("All").trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );

    waypoints = $('#russia').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("RUS").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            } else {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("_ADV").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );


    waypoints = $('#china').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("CHN").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            } else {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("RUS").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );


    waypoints = $('#socialCentralWesternEurope').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(["AUT", "CZE", "DNK", "HUN", "NLD", "POL", "SVK", "SWE", "CHE"]).trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val("top_Social").trigger('change',[true]);
            } else {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("CHN").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );

    waypoints = $('#czechia').waypoint(function(direction) {
            if(direction === 'down') {
                switchMultiSelect('fields',false);
                $('#ddl_countries select').val("CZE").trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val(["top_Life", "top_Social", "top_Physical", "top_Health"]).trigger('change',[true]);
            } else {
                switchMultiSelect('countries',false);
                $('#ddl_countries select').val(["AUT", "CZE", "DNK", "HUN", "NLD", "POL", "SVK", "SWE", "CHE"]).trigger('change',[false]);
                $('#ddl_methods select').val("euclid").trigger('change',[false]);
                $('#ddl_fields select').val("top_Social").trigger('change',[true]);
            }
        },
        {offset:'60%'}
    );

    return waypoints;
}


function MoveOn(selector) {
    var adjust = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 100;
    $('html,body').animate({
        scrollTop: $('#' + selector).offset().top - adjust
    });
}
function showMethodModal(method) {
    showModal('modMethods');
    displayMethod(method);
}

function showModal(modal) {
    $('.modalBackground').fadeIn(200,function() {$('#' + modal).addClass('modalActive')});
}

function hideModal() {
    $('.modalBackground').fadeOut(200,function() {});
    $('.modalActive').removeClass('modalActive')
}

function hideAndShowModal(modal) {
    hideModal();

    showModal(modal);
}

window.onclick = function(event) {
    modal = document.getElementById('modalWrap')
    if (event.target == modal) {
        id = $('.modalActive').attr('id')
        if(id != 'modRozliseni') {
            hideModal();
        }
    }
}

$(document).keyup(function(e) {
    if (e.keyCode === 27) {
        hideModal()
    }   // esc
});



function copyLink(copyInputSelector,copiedNoticeSelector) {
    $(copiedNoticeSelector).hide();
    /* Get the text field */
    var copyText = $(copyInputSelector)[0];

    /* Select the text field */
    copyText.select();

    /* Copy the text inside the text field */
    document.execCommand("copy");

    $(copiedNoticeSelector).fadeIn();
}


function shareLinks() {
    //link = window.location.href;


    //Facebook
    $('#fb').attr('href',"https://www.facebook.com/sharer/sharer.php?u=" + encodeURI(weblink));

    //Twitter
    $('#tw').attr('href',"https://twitter.com/intent/tweet?text=" + encodeURI(webtitle + ' ' + weblink) );

    //LinkedIn
    $('#li').attr('href',"http://www.linkedin.com/shareArticle?mini=true&url=" + encodeURI(weblink) + "&title=" + encodeURI(webtitle))

    $('#mail').attr('href',"mailto:?subject="+ encodeURIComponent(webtitle) + "&body=" + encodeURIComponent(weblink) )
}

function detectIE() {
    var ua = window.navigator.userAgent;

    var msie = ua.indexOf('MSIE ');
    if (msie > 0) {
        // IE 10 or older => return version number
        return parseInt(ua.substring(msie + 5, ua.indexOf('.', msie)), 10);
    }

    var trident = ua.indexOf('Trident/');
    if (trident > 0) {
        // IE 11 => return version number
        var rv = ua.indexOf('rv:');
        return parseInt(ua.substring(rv + 3, ua.indexOf('.', rv)), 10);
    }

    var edge = ua.indexOf('Edge/');
    if (edge > 0) {
        // Edge (IE 12+) => return version number
        return parseInt(ua.substring(edge + 5, ua.indexOf('.', edge)), 10);
    }

    // other browser
    return false;
}
