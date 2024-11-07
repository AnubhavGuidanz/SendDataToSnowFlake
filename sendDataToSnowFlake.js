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

const generateRandomTimestampToday = () => {
  const now = new Date(); 
  const todayDate = new Date(now.toISOString().split('T')[0]);
  const randomHour = chance.integer({ min: 0, max: 23 });
  const randomMinute = chance.integer({ min: 0, max: 59 });
  const randomSecond = chance.integer({ min: 0, max: 59 });
  todayDate.setHours(randomHour, randomMinute, randomSecond, 0);
  const istOffset = 5.5 * 60 * 60 * 1000;
  const istTime = new Date(todayDate.getTime() + istOffset); 
  return istTime.toISOString(); 
};

const generateData = () => {
  const transactionId = generateRandomHash();
  return {
    TRANSACTIONID: transactionId,
    TRANSACTIONAMOUNT: chance.floating({ min: 100, max: 10000 }),
    TRANSACTIONDATE: generateRandomTimestampToday(), 
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
      TRANSACTIONID, TRANSACTIONAMOUNT, TRANSACTIONDATE, TRANSACTIONTYPE,
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
      data.TRANSACTIONDATE,
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
