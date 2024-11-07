const snowflake = require('snowflake-sdk');
const Chance = require('chance');  // Importing the Chance library

const chance = new Chance(); // Create a new instance of Chance

// Snowflake configuration
const connection = snowflake.createConnection({
  account: 'tb62936.ap-south-1',
  username: 'BICUser',
  password: 'Bic@2022',
  warehouse: 'BIC_SMALLWH',
  database: 'NORTHWINDDB',
  schema: 'TRANSACTIONS',
});

connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect to Snowflake:', err.message);
    console.error('Error code:', err.code);
  } else {
    console.log('Successfully connected to Snowflake.');
    insertData();
  }
});

const insertData = () => {
  let transactionId = 2600;
  const values = [];

  // Loop to generate 50 rows of data
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

  // SQL query for batch insert
  const sql = `
    INSERT INTO TRANSACTIONS (
      TRANSACTIONID, TRANSACTIONAMOUNT, TRANSACTIONDATE, TRANSACTIONTYPE, 
      IPADDRESS, CHANNEL, CUSTOMERAGE, CUSTOMEROCCUPATION, 
      TRANSACTIONDURATION, LOGINATTEMPTS, COUNTBALANCE
    )
    VALUES ${values.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')};
  `;

  // Flatten the values array into a single array for binding
  const flatValues = values.flat();

  // Execute the insert query in Snowflake
  connection.execute({
    sqlText: sql,
    binds: flatValues,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to insert data:', err);
      } else {
        console.log('Successfully inserted 50 rows.');
      }

      // Close the connection after insertion
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

// Ensure generateData uses the unique transaction ID
const generateData = (transactionId) => ({
  TRANSACTIONID: transactionId, // Unique transaction ID for each row
  TRANSACTIONAMOUNT: chance.floating({ min: 100, max: 5000, fixed: 2 }), // Random transaction amount
  TRANSACTIONDATE: new Date().toISOString().split('T')[0], // Current date in YYYY-MM-DD format
  TRANSACTIONTYPE: chance.pickone(['Debit', 'Credit', 'Transfer']), // Random transaction type
  IPADDRESS: chance.ip(), // Random IP address
  CHANNEL: chance.pickone(['ATM', 'Online', 'POS']), // Random channel
  CUSTOMERAGE: chance.integer({ min: 18, max: 100 }), // Random customer age
  CUSTOMEROCCUPATION: chance.profession(), // Random occupation
  TRANSACTIONDURATION: chance.integer({ min: 30, max: 300 }), // Random transaction duration
  LOGINATTEMPTS: chance.integer({ min: 1, max: 5 }), // Random login attempts
  COUNTBALANCE: chance.floating({ min: 1000, max: 50000, fixed: 2 }), // Random account balance
});
