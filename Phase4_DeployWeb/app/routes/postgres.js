const { Pool, Client } = require('pg');
const connectionString = 'postgresql://postgres:postgres@db:5432/scienceInternationalitydb';

const pool = new Pool({
    connectionString: connectionString,
});

module.exports = {
    query: (text, params) => pool.query(text, params)
};
