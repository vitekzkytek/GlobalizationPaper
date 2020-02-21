function query_dll_inputs(purpose) {
    let func;
    switch(purpose) {
        case 'fields':
            func = queryddl_fields;
            break;
        case 'countries':
            func = queryddl_countries;
            break;
        case 'methods':
            func = queryddl_methods;
            break;

        default:
            alert('unknown purpose for ajaxing DDL input');
    }
    return func()

}

function queryddl_fields(){
    return {
        url:'/',
        data:{
            id:'queryFields',
            query: {
                level: 'TOP'
            }
        },
    };
}


function queryddl_countries() {
    return {
        url:'/',
        data:{
            id:'queryCountries',
            query: {
            }
        },
    };

}

function queryddl_methods() {
    return {
        url:'/',
        data:{
            id:'queryMethods',
            query: {
            }
        },
    };

}


function retrieveInterindex(req) {
    return {
        url: "/line",
        data: {
            id: 'queryInterindex',
            // query: {
                country_code: req.countries,
                field_code: req.fields,
                method_code: req.methods,
            // },
            req: req
        },
    }
}
