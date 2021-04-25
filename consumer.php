<?php
require 'vendor/autoload.php';
require('phpMQTT.php');
$config = \Kafka\ConsumerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
$config->setMetadataBrokerList('35.213.167.142:9092');
$config->setGroupId('LIVE_MONITOR_EVENTS');
$config->setBrokerVersion('1.0.0');
$config->setTopics(['LIVE_MONITOR_EVENTS']);
$consumer = new \Kafka\Consumer();
$_SESSION['driverData'] = '{"driverList":[{
      "driver_id": "430",
      "driver_name": "Kamal",
      "driver_contact_no": "954545454",
      "lat": 28.45,
      "long": 77.67,
      "current_state": "offline",
      "updated_on": "2019-01-03 20:58:11",
      "dispatch_center_id": "722",
      "device_info": null,
      "app_version_name": "",
      "app_version_code": "0",
      "is_active": "1",
      "gcm_id": null,
      "mapped_driver_id": "1909",
      "car_number": "BR-12 5676",
      "car_model_id": "356",
      "vehicle_type": "car",
      "dispatch_center_name": "VDP_DC",
      "car_model_name": "Prius",
      "unavailable": 1,
      "network_type": "",
      "gps": ""
    },{"driver_id":"1909","driver_name":"Raju","driver_contact_no":"598855555","lat":31.45,"long":77.67,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""},{"driver_id":"2000","driver_name":"ramesh","driver_contact_no":"598855555","lat":31.45,"long":77.434,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}],"countDetails":{"driver_never_logged_in":{"count":0,"data":[]},"driver_logged_out":{"count":0,"data":[]},"driver_unmapped":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":0,"car_number":"","car_model_id":"","vehicle_type":"","dispatch_center_name":"VDP_DC","car_model_name":"","unavailable":1,"network_type":"","gps":""}]},"unknown_offline":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"unknown_offline":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"unknown_onduty":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"driver_onduty":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"driver_online":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"driver_offline":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"internet_unavailable":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"driver_unmapped":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"driver_inactive":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"gps_on":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]},"sos":{"count":1,"data":[{"driver_id":"1909","driver_name":"raju","driver_contact_no":"598855555","lat":null,"long":null,"current_state":"offline","updated_on":"2019-01-03 20:58:11","dispatch_center_id":"722","device_info":null,"app_version_name":"","app_version_code":"0","is_active":"1","gcm_id":null,"mapped_driver_id":"1909","car_number":"BR-12 5676","car_model_id":"356","vehicle_type":"car","dispatch_center_name":"VDP_DC","car_model_name":"Prius","unavailable":1,"network_type":"","gps":""}]}}}';
$_SESSION['countDetails'] = array();
$consumer->start(function($topic, $part, $message) {
	print_r($message);die;
    $username = 'tracker_car_api';                   // set your username
    $password = 'hdjf%^$$#$jdhjdsjhdj6^&^**&^*&KPO)_(KL:<:LKnjg2t3';
    if (empty($mqtt)) {
        $server = 'trkr.myf.io';     // change if necessary
        $port = 1883;                     // change if necessary
        // set your password
        $client_id = 'phpMQTT-publishersafasfsdfsdfsdfsrsdfs'; // make sure this is unique for connecting to sever - you could use uniqid()

        $mqtt = new Bluerhinos\phpMQTT($server, $port, $client_id);
        
    }
    $eventData = json_decode($message['message']['value'], true);
    print_r($eventData);
    $driverListForTest = '{
    "1909":{
      "driver_id": "1909",
      "driver_name": "Raju",
      "driver_contact_no": "598855555",
      "lat": 31.45,
      "long": 77.67,
      "current_state": "offline",
      "updated_on": "2019-01-03 20:58:11",
      "dispatch_center_id": "722",
      "device_info": null,
      "app_version_name": "",
      "app_version_code": "0",
      "is_active": "1",
      "gcm_id": null,
      "mapped_driver_id": "1909",
      "car_number": "BR-12 5676",
      "car_model_id": "356",
      "vehicle_type": "car",
      "dispatch_center_name": "VDP_DC",
      "car_model_name": "Prius",
      "unavailable": 1,
      "network_type": "",
      "gps": ""
    },
    "2000":{
      "driver_id": "2000",
      "driver_name": "ramesh",
      "driver_contact_no": "598855555",
      "lat": 31.45,
      "long": 77.434,
      "current_state": "offline",
      "updated_on": "2019-01-03 20:58:11",
      "dispatch_center_id": "722",
      "device_info": null,
      "app_version_name": "",
      "app_version_code": "0",
      "is_active": "1",
      "gcm_id": null,
      "mapped_driver_id": "1909",
      "car_number": "BR-12 5676",
      "car_model_id": "356",
      "vehicle_type": "car",
      "dispatch_center_name": "VDP_DC",
      "car_model_name": "Prius",
      "unavailable": 1,
      "network_type": "",
      "gps": ""
    },"430":{
      "driver_id": "430",
      "driver_name": "Kamal",
      "driver_contact_no": "954545454",
      "lat": 28.45,
      "long": 77.67,
      "current_state": "offline",
      "updated_on": "2019-01-03 20:58:11",
      "dispatch_center_id": "722",
      "device_info": null,
      "app_version_name": "",
      "app_version_code": "0",
      "is_active": "1",
      "gcm_id": null,
      "mapped_driver_id": "1909",
      "car_number": "BR-12 5676",
      "car_model_id": "356",
      "vehicle_type": "car",
      "dispatch_center_name": "VDP_DC",
      "car_model_name": "Prius",
      "unavailable": 1,
      "network_type": "",
      "gps": ""
    }
  }';
    $driverListForTestArr = json_decode($driverListForTest, true);
    $driverIds = array_keys($driverListForTestArr);
    $driverId = $eventData["data"]["driver_id"];
    if (in_array($driverId, $driverIds)) {
        $driverData = $_SESSION['driverData'];
        $driverList = json_decode($driverData, true);
        if ($eventData['event_name'] == 'add_driver') {     
            if(!in_array($driverId, $_SESSION['countDetails']['driver_never_logged_in'])) {
                $_SESSION['countDetails']['driver_never_logged_in'][$driverId] = $driverId;
                $driverList['countDetails']['driver_never_logged_in']['count']++;
                $driverList['countDetails']['driver_never_logged_in']['data'][] = $driverListForTestArr[$driverId];                        
            }
        }
        if ($eventData['event_name'] == 'logged_in_driver') {
            if(isset($_SESSION['countDetails']['driver_never_logged_in'][$driverId])) {
                unset($_SESSION['countDetails']['driver_never_logged_in'][$driverId]);            
                $driverList['countDetails']['driver_never_logged_in']['data'] = removeDriver($driverId, $driverList['countDetails']['driver_never_logged_in']['data']);            
                $driverList['countDetails']['driver_never_logged_in']['count']--;
            }
            if(isset($_SESSION['countDetails']['driver_logged_out'][$driverId])) {
                unset($_SESSION['countDetails']['driver_logged_out'][$driverId]);
                $driverList['countDetails']['driver_logged_out']['data'] = removeDriver($driverId, $driverList['countDetails']['driver_logged_out']['data']);            
                $driverList['countDetails']['driver_logged_out']['count']--;
            }
            
        }        
        if ($eventData['event_name'] == 'driver_logged_out') {                        
            if(!in_array($driverId, $_SESSION['countDetails']['driver_logged_out'])) {
                $_SESSION['countDetails']['driver_logged_out'][$driverId] = $driverId;
                $driverList['countDetails']['driver_logged_out']['count']++;
                $driverList['countDetails']['driver_logged_out']['data'][] = $driverListForTestArr[$driverId];
            }
        }        
        
        $_SESSION['driverData'] = json_encode($driverList);
        $driverData = $_SESSION['driverData'];
        if ($mqtt->connect(true, NULL, $username, $password)) {
            $mqtt->publish('test_0.26922398500059397_1606888836242', $driverData, 0, false);
            $mqtt->close();
            echo "data published";
        } else {
            echo "Time out!\n";
        }
    }
});

function removeDriver($driverId, $driverList) {
    $driverNewList = array();
    foreach($driverList as $driverData) {
        if($driverData['driver_id'] != $driverId) {
            $driverNewList[] = $driverData;
        }
    } 
    
    return $driverNewList;
}
