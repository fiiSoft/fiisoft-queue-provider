<?php

namespace FiiSoft\Queue\RabbitMQ;

use BadMethodCallException;
use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPOutOfBoundsException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitChannel
{
    /** @var AMQPChannel */
    private $channel;
    
    /** @var string */
    private $exchangeName;
    
    /** @var string */
    private $queueName;
    
    /** @var bool */
    private $messageHandlerSet = false;
    
    /**
     * @param AMQPChannel $channel
     * @param string $exchangeName
     */
    public function __construct(AMQPChannel $channel, $exchangeName)
    {
        $this->channel = $channel;
        $this->exchangeName = $exchangeName;
    }
    
    /**
     * @param AMQPMessage $message
     * @return void
     */
    public function pushMessage(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, $this->exchangeName);
    }
    
    /**
     * @param Closure $handler this function will get one argument of type AMQPMessage each time when
     *                         new message received from queue is consumed
     */
    public function setMessageHandler(Closure $handler)
    {
        if ($this->queueName === null) {
            list($this->queueName) = $this->channel->queue_declare('', false, false, true, false);
            $this->channel->queue_bind($this->queueName, $this->exchangeName);
        }
        
        $this->channel->basic_consume($this->queueName, '', false, true, false, false, $handler);
        $this->messageHandlerSet = true;
    }
    
    /**
     * @param bool $nonBlocking
     * @param integer $timeout 0 means no-timeout
     * @throws AMQPOutOfBoundsException
     * @throws AMQPRuntimeException
     * @throws BadMethodCallException
     * @return bool true if any message was received, false if no message was received
     */
    public function waitForMessage($nonBlocking = false, $timeout = 0)
    {
        if (!$this->messageHandlerSet) {
            throw new BadMethodCallException('MessageHandler must be set before call method waitForMessage');
        }
    
        if ($timeout) {
            try {
                $this->channel->wait(null, $nonBlocking, $timeout);
            } catch (AMQPTimeoutException $notImportant) {
                return false; //there is simply no message available so return with false
            }
        } else {
            $this->channel->wait(null, $nonBlocking);
        }
        
        return true;
    }
}