<?php
try{
require 'vendor/autoload.php';
$config = \Kafka\ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
//$config->setMetadataBrokerList('ec2-3-7-62-5.ap-south-1.compute.amazonaws.com:9092');
$config->setMetadataBrokerList('localhost:9092');
$config->setBrokerVersion('1.0.0');
$config->setRequiredAck(1);
$config->setIsAsyn(false);
$config->setProduceInterval(500);
$producer = new \Kafka\Producer();

$kafkaProducer = new kafkaProducer();
}catch(exception $ex) {
	print_r($ex);die;
}
//17267, 13670
$eventName = 'add_driver';
$cafkaQueueName = 'LIVE_MONITOR_EVENTS';
$driverId = '668';
$bookingId = 102;
$eventNameArray = array('add_driver', 'assign_car');
foreach ($eventNameArray as $eventName) {
    if ($eventName == 'add_driver') {
        $data = array('data' => array('driver_id' => $driverId), 'event_name' => $eventName, 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'assign_car') {
        $data = array('data' => array('driver_id' => $driverId), 'event_name' => $eventName, 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'update_driver') {
        $data = array('data' => array('driver_id' => $driverId), 'event_name' => $eventName, 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'logged_in_driver') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'logged_in_driver') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'driver_online') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'driver_offline') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'driver_offline') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'driver_assignment') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId, 'booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'dispatch_booking') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId, 'booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'arrived') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId, 'booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'start_trip') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId, 'booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'end_trip') {
        $data = array('event_name' => $eventName, 'data' => array('driver_id' => $driverId, 'booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }
    if ($eventName == 'sos') {
        $data = array('event_name' => $eventName, 'data' => array('booking_id' => $bookingId), 'request_datetime' => date('Y-m-d H:i:s'));
    }

    $datajson = json_encode($data);
    
    $producer->send([
        [
            'topic' => $cafkaQueueName,
            'value' => $datajson,
            'key' => '',
        ],
    ]);
}

//$mysqli = new mysql_connect();

//"34.87.159.178", 'dev_db_user_algoscale', 'G:]xj/a4`*DJyW7Uhh-;caE_A=:wBw/h', 'db_cab_management'
/*$hostname = "34.87.159.178:3306";
$username = "dev_db_user_algoscale";
$password = "G:]xj/a4`*DJyW7Uhh-;caE_A=:wBw/h";

/*$hostname = "localhost:3306";
$username = "root";
$password = "ravi@321";
$conn = mysqli_connect($hostname, $username, $password) OR DIE ("Unable to connect to database!");
print_r($conn);die;*/

array(
    array('driver_id'=>1, 'event_list'=>array('add_driver')),
    array('driver_id'=>2, 'event_list'=>array('loggedin_driver')),
);

array(
    array('event_name'=>'add_driver','driver_id'=>1,'request_time'=>'2020-11-26 14:41:00'),
    array('event_name'=>'add_driver','driver_id'=>2,'request_time'=>'2020-11-26 14:41:10'),
    array('event_name'=>'loggedin_driver','driver_id'=>1,'request_time'=>'2020-11-26 15:41:10'),
    array('event_name'=>'assign_car','driver_id'=>1,'request_time'=>'2020-11-26 15:45:10'),
    array('event_name'=>'driver_assignment','driver_id'=>1,'booking_id'=>'112','request_time'=>'2020-11-26 15:45:10'),
    array('event_name'=>'loggedin_driver','driver_id'=>2,'request_time'=>'2020-11-26 18:41:10'),
    array('event_name'=>'loggedin_driver','driver_id'=>2,'request_time'=>'2020-11-26 18:41:10'),
);
