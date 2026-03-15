import { client } from "../config/dbcon.js";


// GET all finance records
export const getAllFinanceRecords = async (req, res) => {
  try {
    console.log('=== GET ALL FINANCE RECORDS ===');
    const result = await client.query(
      'SELECT * FROM finance_records ORDER BY created_at DESC'
    );
    console.log('Total records fetched:', result.rows.length);
    res.json({ data: result.rows });
  } catch (error) {
    console.error('=== ERROR IN GET ALL FINANCE RECORDS ===');
    console.error('Error message:', error.message);
    console.error('Error details:', error);
    res.status(500).json({ error: 'Failed to fetch finance records', details: error.message });
  }
};

// POST create finance record
export const createFinanceRecord = async (req, res) => {
  try {
    const { amount, description, type, category, date } = req.body;
    console.log('=== CREATE FINANCE RECORD ===');
    console.log('Request body:', { amount, description, type, category, date });
    
    if (!amount || !type || !date) {
      console.log('Validation failed - Missing required fields');
      return res.status(400).json({ error: 'Missing required fields: amount, type, date' });
    }

    console.log('Validation passed, attempting database insert...');
    const result = await client.query(
      'INSERT INTO finance_records (amount, description, type, category, date) VALUES ($1, $2, $3, $4, $5) RETURNING *',
      [amount, description, type, category, date]
    );
    console.log('Database insert successful:', result.rows[0]);
    res.status(201).json({ data: result.rows[0] });
  } catch (error) {
    console.error('=== ERROR IN CREATE FINANCE RECORD ===');
    console.error('Error message:', error.message);
    console.error('Error code:', error.code);
    console.error('Error details:', error);
    console.error('Full error object:', JSON.stringify(error, null, 2));
    res.status(500).json({ error: 'Failed to create finance record', details: error.message });
  }
};

// PUT update finance record
export const updateFinanceRecord = async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, description, type, category, date } = req.body;
    console.log('=== UPDATE FINANCE RECORD ===');
    console.log('Record ID:', id);
    console.log('Update data:', { amount, description, type, category, date });
    
    const result = await client.query(
      'UPDATE finance_records SET amount = $1, description = $2, type = $3, category = $4, date = $5 WHERE id = $6 RETURNING *',
      [amount, description, type, category, date, id]
    );
    if (result.rows.length === 0) {
      console.log('Record not found for ID:', id);
      return res.status(404).json({ error: 'Finance record not found' });
    }
    console.log('Record updated successfully:', result.rows[0]);
    res.json({ data: result.rows[0] });
  } catch (error) {
    console.error('=== ERROR IN UPDATE FINANCE RECORD ===');
    console.error('Error message:', error.message);
    console.error('Error details:', error);
    res.status(500).json({ error: 'Failed to update finance record', details: error.message });
  }
};

// DELETE finance record
export const deleteFinanceRecord = async (req, res) => {
  try {
    const { id } = req.params;
    console.log('=== DELETE FINANCE RECORD ===');
    console.log('Deleting record with ID:', id);
    
    const result = await client.query(
      'DELETE FROM finance_records WHERE id = $1 RETURNING id',
      [id]
    );
    if (result.rows.length === 0) {
      console.log('Record not found for deletion with ID:', id);
      return res.status(404).json({ error: 'Finance record not found' });
    }
    console.log('Record deleted successfully, ID:', result.rows[0].id);
    res.json({ data: { id: result.rows[0].id } });
  } catch (error) {
    console.error('=== ERROR IN DELETE FINANCE RECORD ===');
    console.error('Error message:', error.message);
    console.error('Error details:', error);
    res.status(500).json({ error: 'Failed to delete finance record', details: error.message });
  }
};

// POST add payment to finance record
export const addPayment = async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, payment_date, payment_method } = req.body;
    console.log('=== ADD PAYMENT ===');
    console.log('Finance record ID:', id);
    console.log('Payment data:', { amount, payment_date, payment_method });

    if (!amount || !payment_date) {
      console.log('Validation failed - Missing required fields');
      return res.status(400).json({ error: 'Missing required fields: amount, payment_date' });
    }

    console.log('Validation passed, attempting to insert payment...');
    const result = await client.query(
      'INSERT INTO payments (finance_record_id, amount, payment_date, payment_method) VALUES ($1, $2, $3, $4) RETURNING *',
      [id, amount, payment_date, payment_method]
    );
    console.log('Payment added successfully:', result.rows[0]);
    res.status(201).json({ data: result.rows[0] });
  } catch (error) {
    console.error('=== ERROR IN ADD PAYMENT ===');
    console.error('Error message:', error.message);
    console.error('Error code:', error.code);
    console.error('Error details:', error);
    res.status(500).json({ error: 'Failed to add payment', details: error.message });
  }
};
