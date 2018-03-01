<?php
namespace Google\Cloud\Samples\Spanner;
use Google\Cloud\Spanner\SpannerClient;
# Include the autoloader for libraries installed with composer
require __DIR__ . '/vendor/autoload.php';
/*
Uasge:
php ycsb.php
  --operationcount={number of operations} \
  --instance=[gcloud instance] \
  --database={database name} \
  --table={table to use} \
  --workload={workload file}

Note: all arguments above are mandatory
Note: This bnchmark script assumes that the table has a PK field named "id".

*/

$msg = "";
$arrKEYS = [];
$arrOPERATIONS = ['readproportion', 'updateproportion', 'scanproportion', 'insertproportion'];

# if zero, then don't process/use this table at all.
AGGREGATE_BALANCE_SHARDS = 16
class NegativeBalance {
	}
class RowAlreadyUpdated{
	}
class NoResults{
	}
class TooManyResults{
	}
class Unsupported{
	}

function generate_int64() {
	// Should check at some point that PHP can support such a large number.
	// Since this is used for a "bank account" number, it may behoove us to just generate a string instead.
	return rand(0, (1<<63)-1);
	}

function generate_customer_number() {
	return generate_int64();
	}

function generate_account_number() {
	return generate_int64();
	}

$CUSTOMERS = array();
$ACCOUNTS = array();

for (i = 0; i < 5; i++) {
	$CUSTOMERS[] = generate_customer_number();
	$ACCOUNTS[] = generate_account_number();
	}

function clear_tables($database) {
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('AccountHistory', )
        ->commit();
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('Accounts', )
        ->commit();
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('Customers', )
        ->commit();
	if ($AGGREGATE_BALANCE_SHARDS > 0) {
		$operation = $database->transaction(['singleUse' => false])
        	->deleteBatch('AggregateBalance', )
			->commit();
		}
	}

function setup_customers($database) {
	clear_tables($database);
	
	$table = "Customers";
	$values = array(
		array('CustomerNumber'=>$CUSTOMERS[0], 'FirstName'=>'Marc', 'LastName'=>'Richards'),
		array('CustomerNumber'=>$CUSTOMERS[1], 'FirstName'=>'Catalina', 'LastName'=>'Smith'),
		array('CustomerNumber'=>$CUSTOMERS[2], 'FirstName'=>'Alice', 'LastName'=>'Trentor'),
		array('CustomerNumber'=>$CUSTOMERS[3], 'FirstName'=>'Lea', 'LastName'=>'Martin'),
		array('CustomerNumber'=>$CUSTOMERS[4], 'FirstName'=>'David', 'LastName'=>'Lomond')
		);
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	$table = "Accounts";
	$values = array(
		array('CustomerNumber'=>$CUSTOMERS[0], 'AccountNumber'=>$ACCOUNTS[0], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[1], 'AccountNumber'=>$ACCOUNTS[1], 'AccountType'=>1, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[2], 'AccountNumber'=>$ACCOUNTS[2], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[3], 'AccountNumber'=>$ACCOUNTS[3], 'AccountType'=>1, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[4], 'AccountNumber'=>$ACCOUNTS[4], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL)
		);
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	$table = "AccountHistory";
	$values = array();
	foreach ($ACCOUNTS as $a) {
		$values[] = array('AccountNumber'=>$a, 'Ts'=>date(DATE_ATOM, time()), 'ChangeAmount'=>0, 'Memo'=>'New Account Initial Deposit')
		}
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	if ($AGGREGATE_BALANCE_SHARDS > 0) {
		$table = 'AggregateBalance';
		$values = array();
		for ($i = 0; $i < $AGGREGATE_BALANCE_SHARDS; $i++) {
			$values[] = array('Shard'=>$i, 'Balance'=>0);
			}
		$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();
		}
	print "Inserted Data."
	}

function extract_single_row_to_tuple($results) {
	
}

function extract_single_cell($results) {
	
}

function account_balance($database, $account_number) {
	
}

function customer_balance($database, $customer_number) {
	
}


?>
