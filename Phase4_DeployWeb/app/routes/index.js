var express = require('express');
var router = express.Router();


const pgres = require('../routes/postgres.js');



router.post('/map',function (req,res,next) {
    let quer = 'select i.country_code, i.value, c.name from interindex i \n' +
        'inner join country c on c.country_code = i.country_code \n' +
        'where i.method_code = $1 and i.field_code = $2 and i.period = $3 and c.type = \'country\'';

    let resp = pgres.query(quer, [req.body.method_code,req.body.field_code,req.body.period]);
    resp.then(result => {
        res.send(result)}
    ).catch(result => {
        res.send('Map Query failed! ' + result)
    })
});

router.post('/line',function (req,res,next) {
    let quer = 'select period,value from interindex i \n' +
    'where i.field_code = $1 and i.country_code = $2 and method_code = $3';

    let resp = pgres.query(quer, [req.body.field_code,req.body.country_code,req.body.method_code]);
    resp.then(result => {
        res.send(result)}
    ).catch(result => {
        res.send('Map Query failed! ' + result)
    })
});



module.exports = router;
