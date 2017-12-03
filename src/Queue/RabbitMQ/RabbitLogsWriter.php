<?php

namespace FiiSoft\Queue\RabbitMQ;

use FiiSoft\Logger\Writer\LogsWriter;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;

final class RabbitLogsWriter implements LogsWriter
{
    /** @var RabbitChannel */
    private $channel;
    
    /** @var AMQPMessage */
    private $message;
    
    /** @var RabbitConnection */
    private $connection;
    
    /**
     * @param RabbitConnection $connection
     */
    public function __construct(RabbitConnection $connection)
    {
        $this->connection = $connection;
        $this->message = new AMQPMessage();
    }
    
    /**
     * @param string $message
     * @param array $context
     * @throws RuntimeException
     * @return void
     */
    public function write($message, array $context = [])
    {
        if (!$this->channel) {
            $this->channel = $this->connection->getLogsChannel();
        }
        
        $body = json_encode(['message' => $message, 'context' => $context]);
        if ($body === false) {
            throw new RuntimeException('Encoding message failed');
        }
    
        $this->channel->pushMessage($this->message->setBody($body));
    }
}