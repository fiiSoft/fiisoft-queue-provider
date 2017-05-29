<?php

namespace FiiSoft\Tools\Queue\RabbitMQ;

use BadMethodCallException;
use FiiSoft\Tools\Logger\Reader\LogConsumer;
use FiiSoft\Tools\Logger\Reader\LogsReader;
use InvalidArgumentException;
use PhpAmqpLib\Exception\AMQPOutOfBoundsException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use UnexpectedValueException;

final class RabbitLogsReader implements LogsReader
{
    /** @var RabbitChannel */
    private $channel;
    
    /** @var LogConsumer */
    private $logConsumer;
    
    /** @var int counter of consumed messages */
    private $counter = 0;
    
    /** @var integer number of messages to consume */
    private $maxReads;
    
    /** @var bool flag to tell if continue consuming logs */
    private $continue = true;
    
    /** @var RabbitConnection */
    private $connection;
    
    /**
     * @param RabbitConnection $connection
     */
    public function __construct(RabbitConnection $connection)
    {
        $this->connection = $connection;
    }
    
    /**
     * @param LogConsumer $logConsumer
     * @param integer|null $maxReads number of logs to consume before return
     * @param integer $timeout maximum time (in seconds) to wait for any log before return; 0 means "no timeout"
     * @throws InvalidArgumentException if param maxReads is invalid
     * @throws AMQPOutOfBoundsException
     * @throws AMQPRuntimeException
     * @throws UnexpectedValueException
     * @throws BadMethodCallException
     * @throws RuntimeException
     * @return void
     */
    public function read(LogConsumer $logConsumer, $maxReads = null, $timeout = 0)
    {
        if ($maxReads === null) {
            $maxReads = 0; //it means no limit
        } elseif (!is_int($maxReads) || $maxReads < 1) {
            throw new InvalidArgumentException('Invalid param maxReads');
        }
    
        if (!is_int($timeout) || $timeout < 0) {
            throw new InvalidArgumentException('Invalid param timeout');
        }
    
        if (!$this->channel) {
            $this->channel = $this->connection->getLogsChannel();
        }
    
        $this->continue = true;
        $this->maxReads = $maxReads;
        
        if ($this->logConsumer !== $logConsumer) {
            $this->setLogConsumerAsMessageHandler($logConsumer);
        }
    
        if ($timeout !== 0) {
            do {
                if (!$this->channel->waitForMessage(true, $timeout)) {
                    break;
                }
            } while ($this->continue);
        } elseif ($this->maxReads !== 0) {
            do {
                $this->channel->waitForMessage(true);
            } while ($this->continue);
        } else {
            while (true) {
                $this->channel->waitForMessage();
            }
        }
    }
    
    /**
     * @param LogConsumer $logConsumer
     * @throws UnexpectedValueException
     * @return void
     */
    private function setLogConsumerAsMessageHandler(LogConsumer $logConsumer)
    {
        //TODO check what will happen on change of consumer during work
        
        $this->logConsumer = $logConsumer;
        $this->counter = 0;
    
        $this->channel->setMessageHandler(function (AMQPMessage $message) {
            if ($this->maxReads !== 0 && ++$this->counter === $this->maxReads) {
                $this->continue = false;
                $this->counter = 0;
            }
        
            $decoded = json_decode($message->body, true);
            if ($decoded !== false && isset($decoded['message'], $decoded['context'])) {
                $this->logConsumer->consumeLog($decoded['message'], $decoded['context']);
            } else {
                throw new UnexpectedValueException('Invalid format of received message: '.$message->body);
            }
        });
    }
}