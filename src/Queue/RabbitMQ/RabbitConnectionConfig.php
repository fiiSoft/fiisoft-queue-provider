<?php

namespace FiiSoft\Queue\RabbitMQ;

use FiiSoft\Tools\Configuration\AbstractConfiguration;

final class RabbitConnectionConfig extends AbstractConfiguration
{
    /**
     * @var string user's login
     */
    public $user = 'guest';
    
    /**
     * @var string user's password
     */
    public $password = 'guest';
    
    /**
     * @var string location of server
     */
    public $host = 'localhost';
    
    /**
     * @var int connection port
     */
    public $port = 5672;
    
    /**
     * @var string name of exchange used to write and read logs
     */
    public $exchangeForLogs = 'logs';
    
    /**
     * @var string name of main queue used to send messages to process tasks
     */
    public $queueForTasks = 'tasks_queue';
}