<?php

namespace FiiSoft\Tools\Queue\RabbitMQ;

use Exception;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use RuntimeException;

final class RabbitConnection
{
    /** @var AMQPStreamConnection */
    private $connection;

    /** @var RabbitChannel */
    private $channelLogs;
    
    /** @var RabbitLogsReader */
    private $logsReader;
    
    /** @var RabbitLogsWriter */
    private $logsWriter;
    
    /** @var RabbitQueue */
    private $tasksQueue;
    
    /** @var RabbitConnectionConfig */
    private $config;
    
    /**
     * @param RabbitConnectionConfig $config
     */
    public function __construct(RabbitConnectionConfig $config)
    {
        $this->config = clone $config;
    }
    
    public function __destruct()
    {
        if ($this->connection) {
            $this->connection->close();
        }
    }
    
    /**
     * @throws RuntimeException
     * @return RabbitLogsReader
     */
    public function getLogsReader()
    {
        if ($this->logsReader === null) {
            $this->logsReader = new RabbitLogsReader($this);
        }
        
        return $this->logsReader;
    }
    
    /**
     * @throws RuntimeException
     * @return RabbitLogsWriter
     */
    public function getLogsWriter()
    {
        if ($this->logsWriter === null) {
            $this->logsWriter = new RabbitLogsWriter($this);
        }
        
        return $this->logsWriter;
    }
    
    /**
     * @throws RuntimeException
     * @return RabbitChannel
     */
    public function getLogsChannel()
    {
        if ($this->channelLogs === null) {
            $this->connect();
            $echangeName = $this->config->exchangeForLogs;
            
            $channel = $this->connection->channel();
            $channel->exchange_declare($echangeName, 'fanout', false, false, false);
        
            $this->channelLogs = new RabbitChannel($channel, $echangeName);
        }
        
        return $this->channelLogs;
    }
    
    /**
     * @throws RuntimeException
     * @return RabbitQueue
     */
    public function getTasksQueue()
    {
        if ($this->tasksQueue === null) {
            $this->connect();
            $queueName = $this->config->queueForTasks;
            
            $channel = $this->connection->channel();
            $channel->queue_declare($queueName, false, true, false, false);
            $channel->basic_qos(null, 1, null);
            
            $this->tasksQueue = new RabbitQueue($channel, $queueName);
        }
        
        return $this->tasksQueue;
    }
    
    /**
     * @throws RuntimeException
     * @return void
     */
    private function connect()
    {
        if ($this->connection === null) {
            try {
                $this->connection = new AMQPStreamConnection(
                    $this->config->host,
                    $this->config->port,
                    $this->config->user,
                    $this->config->password
                );
            } catch (Exception $e) {
                throw new RuntimeException(
                    'Cannot establish connection to RabbitMQ server. More info:'.$e->getMessage(),
                    $e->getCode(),
                    $e
                );
            }
        }
    }
}