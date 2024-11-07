const fs = require('fs');
const snowflake = require('snowflake-sdk');
const Chance = require('chance'); 

const chance = new Chance(); 

const path = './transactionId.json';  

const connection = snowflake.createConnection({
  account: 'tb62936.ap-south-1',
  username: 'BICUser',
  password: 'Bic@2022',
  warehouse: 'BIC_SMALLWH',
  database: 'NORTHWINDDB',
  schema: 'TRANSACTIONS',
});

const getCurrentTransactionId = () => {
  try {
    const data = fs.readFileSync(path, 'utf8');
    return JSON.parse(data).lastTransactionId || 2600;  
  } catch (err) {
    console.error('Error reading the file:', err);
    return 2600;  
  }
};

const updateTransactionId = (newId) => {
  const data = JSON.stringify({ lastTransactionId: newId }, null, 2);
  fs.writeFileSync(path, data, 'utf8');
};

const generateData = (transactionId) => ({
  TRANSACTIONID: transactionId,  
  TRANSACTIONAMOUNT: chance.floating({ min: 100,max:10000 }),  
  TRANSACTIONDATE: new Date().toISOString().split('T')[0],  
  TRANSACTIONTYPE: chance.pickone(['Debit', 'Credit', 'Transfer']),  
  IPADDRESS: chance.ip(),  
  CHANNEL: chance.pickone(['ATM', 'Online', 'POS']),  
  CUSTOMERAGE: chance.integer({ min: 0 ,max:80}),  
  CUSTOMEROCCUPATION: chance.profession(),  
  TRANSACTIONDURATION: chance.integer({min:10,max:200}),  
  LOGINATTEMPTS: chance.integer({min:1,max:5}),  
  COUNTBALANCE: chance.floating({min:10000,max:100000}),
});

const insertData = () => {
  let transactionId = getCurrentTransactionId();
  const values = [];

  for (let i = 0; i < 50; i++) {
    const data = generateData(transactionId++);
    values.push([
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
      data.COUNTBALANCE,
    ]);
  }

  const sql = `
    INSERT INTO TRANSACTIONS (
      TRANSACTIONID, TRANSACTIONAMOUNT, TRANSACTIONDATE, TRANSACTIONTYPE, 
      IPADDRESS, CHANNEL, CUSTOMERAGE, CUSTOMEROCCUPATION, 
      TRANSACTIONDURATION, LOGINATTEMPTS, COUNTBALANCE
    )
    VALUES ${values.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')};
  `;

  const flatValues = values.flat();

  connection.execute({
    sqlText: sql,
    binds: flatValues,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to insert data:', err);
      } else {
        console.log('Successfully inserted 50 rows.');
      }

      updateTransactionId(transactionId);

      connection.destroy((err) => {
        if (err) {
          console.error('Error closing connection: ' + err.message);
        } else {
          console.log('Connection closed successfully.');
        }
      });
    },
  });
};

connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect to Snowflake:', err.message);
    console.error('Error code:', err.code);
  } else {
    console.log('Successfully connected to Snowflake.');
    insertData();  
  }
});
