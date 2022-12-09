import { ConnectionPool } from 'tinyorm';

const pool = new ConnectionPool({
    minSize: 5,
    maxSize: 10,

    primary: {
        host: 'localhost',
        port: 5432,
    },
});

async function main() {
    //...
}

main().catch(err => {
    console.error(err)
    process.exit(1)
})
