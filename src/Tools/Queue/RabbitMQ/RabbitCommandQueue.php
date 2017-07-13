<?php

namespace FiiSoft\Tools\Queue\RabbitMQ;

use FiiSoft\Tools\Logger\Writer\SmartLogger;
use FiiSoft\Tools\TasksQueue\Command;
use FiiSoft\Tools\TasksQueue\CommandMemo;
use FiiSoft\Tools\TasksQueue\CommandQueue;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LogLevel;
use RuntimeException;

final class RabbitCommandQueue implements CommandQueue
{
    /** @var array context for Psr logger */
    private $logContext = ['source' => 'queue'];
    
    /** @var AMQPMessage[] map of commands to messages, used to sending acknowledges */
    private $messages = array();
    
    /** @var RabbitQueue */
    private $tasksQueue;
 
    /** @var AMQPMessage */
    private $message;
    
    /** @var SmartLogger */
    private $logger;
    
    /** @var RabbitConnection */
    private $connection;
    
    /**
     * @param RabbitConnection $connection
     * @param SmartLogger $logger
     * @throws RuntimeException
     */
    public function __construct(RabbitConnection $connection, SmartLogger $logger)
    {
        $this->connection = $connection;
        $this->logger = $logger;
        $this->message = new AMQPMessage();
        
        $this->logger->setPrefix('[Q] ')->setContext($this->logContext);
    }
    
    /**
     * Wait until next new command is ready to be handled and then return it.
     *
     * @param bool $wait
     * @throws RuntimeException
     * @return Command|null
     */
    public function getNextCommand($wait = true)
    {
        if (!$this->tasksQueue) {
            $this->initializeTasksQueue();
        }
        
        $message = $this->tasksQueue->nextMessage($wait);
        if ($message) {
            $memo = unserialize($message->body);
            if ($memo instanceof CommandMemo) {
                $command = $memo->restoreCommand();
                $this->logActivity('Command received: '.$command->getName());
                
                $this->messages[spl_object_hash($command)] = $message;
                return $command;
            }
            
            $this->logError(
                'Unserialized message is not a CommandMemo: '
                ."\n".print_r($memo, true)
                ."\n".'Message body: '
                ."\n".$message->body
            );
        }
    }
    
    /**
     * Confirm that given command has been handled correctly.
     *
     * @param Command $command
     * @return void
     */
    public function confirmCommandHandled(Command $command)
    {
        $key = spl_object_hash($command);
        
        if (isset($this->messages[$key])) {
            $message = $this->messages[$key];
            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            unset($this->messages[$key]);
            $this->logActivity('Command confirmed: '.$command->getName());
        } else {
            $this->logWarning('Command '.$command->getName().' should be confirmed but was not found');
        }
    }
    
    /**
     * @param Command $command
     * @throws RuntimeException
     * @return void
     */
    public function requeueCommand(Command $command)
    {
        $key = spl_object_hash($command);
        
        if (isset($this->messages[$key])) {
//            $message = $this->commands[$key];
//            $message->delivery_info['channel']->basic_reject($message->delivery_info['delivery_tag'], true);
//            unset($this->commands[$key]);
            
            $this->confirmCommandHandled($command);
            $this->publishCommand($command);
            
            $this->logActivity('Command requeued: '.$command->getName());
        } else {
            $errorMsg = 'Command ' . $command->getName() . ' should be requeued but was not found';
            $this->logError($errorMsg);
            throw new RuntimeException($errorMsg);
        }
    }
    
    /**
     * Publish command (send it to queue to execute by worker).
     *
     * @param Command $command
     * @throws RuntimeException
     * @return void
     */
    public function publishCommand(Command $command)
    {
        if (!$this->tasksQueue) {
            $this->initializeTasksQueue();
        }
        
        $this->tasksQueue->pushMessage($this->message->setBody(serialize($command->getMemo())));
        $this->logActivity('Command published: '.$command->getName());
    }
    
    /**
     * Get name of queue.
     *
     * @throws RuntimeException
     * @return string
     */
    public function queueName()
    {
        if (!$this->tasksQueue) {
            $this->initializeTasksQueue();
        }
        
        return $this->tasksQueue->queueName();
    }
    
    /**
     * @throws RuntimeException
     * @return void
     */
    private function initializeTasksQueue()
    {
        $this->tasksQueue = $this->connection->getTasksQueue();
    }
    
    /**
     * Set minimal level of messages logged by logger.
     *
     * @param string $minLevel
     * @return void
     */
    public function setMinimalLogLevel($minLevel)
    {
        $this->logger->setMinLevel($minLevel);
    }
    
    /**
     * @param string $message
     * @return void
     */
    private function logActivity($message)
    {
        $this->log($message, 'queue');
    }
    
    /**
     * @param string $message
     * @return void
     */
    private function logWarning($message)
    {
        $this->log($message, LogLevel::WARNING);
    }
    
    /**
     * @param string $message
     * @return void
     */
    private function logError($message)
    {
        $this->log($message, LogLevel::ERROR);
    }
    
    /**
     * @param string $message
     * @param string $level
     * @return void
     */
    private function log($message, $level)
    {
        $this->logger->log($level, $message);
    }
}