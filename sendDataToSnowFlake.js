const snowflake = require('snowflake-sdk');
const Chance = require('chance');
const crypto = require('crypto');

const chance = new Chance();

const connection = snowflake.createConnection({
  account: 'tb62936.ap-south-1',
  username: 'BICUser',
  password: 'Bic@2022',
  warehouse: 'BIC_SMALLWH',
  database: 'NORTHWINDDB',
  schema: 'TRANSACTION',
});

const generateRandomHash = () => crypto.randomBytes(16).toString('hex');
const generateTimestampCurrentDate = () => {
  const now = new Date(); 
  const todayDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const randomHour = Math.floor(Math.random() * 24);
  const randomMinute = Math.floor(Math.random() * 60);
  const randomSecond = Math.floor(Math.random() * 60);
  todayDate.setUTCHours(randomHour, randomMinute, randomSecond, 0);

  return todayDate.toISOString(); 
};


const generateData = () => {
  const transactionId = generateRandomHash();
  return {
    TRANSACTIONID: transactionId,
    TRANSACTIONAMOUNT: chance.floating({ min: 100, max: 10000 }),
    TIMESTAMP: generateTimestampCurrentDate(), 
    TRANSACTIONTYPE: chance.pickone(['Debit', 'Credit', 'Transfer']),
    IPADDRESS: chance.ip(),
    CHANNEL: chance.pickone(['ATM', 'Online', 'POS']),
    CUSTOMERAGE: chance.integer({ min: 18, max: 80 }),
    CUSTOMEROCCUPATION: chance.pickone(['Student', 'Engineer', 'Doctor', 'Retired', 'Business', 'Pilot', 'Bank Manager', 'ShopKeeper', 'Agent']),
    TRANSACTIONDURATION: chance.integer({ min: 10, max: 200 }),
    LOGINATTEMPTS: chance.integer({ min: 1, max: 5 }),
    COUNTBALANCE: chance.floating({ min: 10000, max: 100000 })
  };
};

const insertData = (transactionValues, customerValues) => {
  const transactionSql = `
    INSERT INTO TRANSACTIONS (
      TRANSACTIONID, TRANSACTIONAMOUNT, TIMESTAMP, TRANSACTIONTYPE,
      IPADDRESS, CHANNEL, CUSTOMERAGE, CUSTOMEROCCUPATION,
      TRANSACTIONDURATION, LOGINATTEMPTS, COUNTBALANCE
    )
    VALUES ${transactionValues.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')};`;

  const customerSql = `
    INSERT INTO CUSTOMERDETAILS (
      TRANSACTIONID, CUSTOMERAGE, CUSTOMEROCCUPATION, COUNTBALANCE
    )
    VALUES ${customerValues.map(() => '(?, ?, ?, ?)').join(', ')};`;

  const flatTransactionValues = transactionValues.flat();
  const flatCustomerValues = customerValues.flat();

  connection.execute({
    sqlText: transactionSql,
    binds: flatTransactionValues,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to insert into TRANSACTIONS table:', err);
      } else {
        console.log('Successfully inserted into TRANSACTIONS table.');

        connection.execute({
          sqlText: customerSql,
          binds: flatCustomerValues,
          complete: (err, stmt, rows) => {
            if (err) {
              console.error('Failed to insert into CUSTOMERDETAILS table:', err);
            } else {
              console.log('Successfully inserted into CUSTOMERDETAILS table.');
            }
            connection.destroy((err) => {
              if (err) {
                console.error('Error closing connection:', err.message);
              } else {
                console.log('Connection closed successfully.');
              }
            });
          },
        });
      }
    },
  });
};

const generateAndInsertData = () => {
  const transactionValues = [];
  const customerValues = [];

  for (let i = 0; i < 50; i++) {
    const data = generateData();
    transactionValues.push([
      data.TRANSACTIONID,
      data.TRANSACTIONAMOUNT,
      data.TIMESTAMP,
      data.TRANSACTIONTYPE,
      data.IPADDRESS,
      data.CHANNEL,
      data.CUSTOMERAGE,
      data.CUSTOMEROCCUPATION,
      data.TRANSACTIONDURATION,
      data.LOGINATTEMPTS,
      data.COUNTBALANCE
    ]);

    customerValues.push([
      data.TRANSACTIONID,
      data.CUSTOMERAGE,
      data.CUSTOMEROCCUPATION,
      data.COUNTBALANCE
    ]);
  }

  insertData(transactionValues, customerValues);
};

connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect to Snowflake:', err.message);
  } else {
    console.log('Successfully connected to Snowflake.');
    generateAndInsertData();
  }
});