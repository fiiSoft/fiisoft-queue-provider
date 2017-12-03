<?php

namespace FiiSoft\Queue\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPOutOfBoundsException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitQueue
{
    /** @var AMQPChannel */
    private $channel;
    
    /** @var string */
    private $queueName;
    
    /** @var string */
    private $consumerTag;
    
    /** @var AMQPMessage */
    private $message;
    
    /**
     * @param AMQPChannel $channel
     * @param string $queueName
     */
    public function __construct(AMQPChannel $channel, $queueName)
    {
        $this->channel = $channel;
        $this->queueName = $queueName;
    }
    
    /**
     * @param bool $wait (default true) if true then it waits until command is available to return
     * @throws AMQPOutOfBoundsException
     * @throws AMQPRuntimeException
     * @return AMQPMessage|null
     */
    public function nextMessage($wait = true)
    {
        if ($this->consumerTag === null) {
            $this->consumerTag = $this->channel->basic_consume(
                $this->queueName, '', false, false, false, false, function (AMQPMessage $msg) {
                    $this->message = $msg;
                }
            );
        }
    
        if ($wait) {
            $this->channel->wait();
        } else {
            try {
                $this->channel->wait(null, true, 1);
            } catch (AMQPTimeoutException $notImportant) {
                return null;
            }
        }
        
        if ($this->message !== null) {
            $message = $this->message;
            $this->message = null;
            return $message;
        }
    }
    
    /**
     * @param AMQPMessage $message
     * @return void
     */
    public function pushMessage(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, '', $this->queueName);
    }
    
    /**
     * @return string
     */
    public function queueName()
    {
        return $this->queueName;
    }
}