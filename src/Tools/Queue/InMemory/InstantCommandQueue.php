<?php

namespace FiiSoft\Tools\Queue\InMemory;

use FiiSoft\Tools\Logger\Writer\SmartLogger;
use FiiSoft\Tools\TasksQueue\Command;
use FiiSoft\Tools\TasksQueue\CommandQueue;
use LogicException;
use Psr\Log\LogLevel;
use SplQueue;

final class InstantCommandQueue implements CommandQueue
{
    /** @var array context for Psr logger */
    private $logContext = ['source' => 'queue'];
    
    /** @var SmartLogger */
    private $logger;
    
    /** @var SplQueue */
    private $storage;
    
//    /** @var array */
//    private $fetched = [];
    
    /**
     * @param SmartLogger $logger
     */
    public function __construct(SmartLogger $logger)
    {
        $this->logger = $logger;
        $this->logger->setPrefix('[Q] ')->setContext($this->logContext);
        $this->storage = new SplQueue();
    }
    
    /**
     * Wait until next new command is ready to be handled and then return it.
     *
     * @param bool $wait (default true) if true then it's blocking operation - waits for available command
     * @throws LogicException
     * @return Command|null can return null only if in non-blocking mode (param $wait is false)
     */
    public function getNextCommand($wait = true)
    {
        if ($wait) {
            throw new LogicException(
                'Synchronous in-memory implementation of CommandQueue cannot operate in blocking mode'
            );
        }
    
        if (!$this->storage->isEmpty()) {
            return $this->storage->dequeue();
        }
        
//        if (!$this->storage->isEmpty()) {
//            $command = $this->storage->dequeue();
//            $this->fetched[spl_object_hash($command)] = $command;
//            return $command;
//        }
    }
    
    /**
     * Confirm that this command has been handled.
     *
     * @param Command $command
     * @return void
     */
    public function confirmCommandHandled(Command $command)
    {
        $this->logActivity('Command confirmed: '.$command->getName());
        
//        $key = spl_object_hash($command);
//        if (isset($this->fetched[$key])) {
//            unset($this->fetched[$key]);
//            $this->logActivity('Command confirmed: '.$command->getName());
//        } else {
//            $this->logWarning('Command '.$command->getName().' should be confirmed but was not found');
//        }
    }
    
    /**
     * Requeue command in case when its execution failed or it cannot be handled properly in this time.
     *
     * @param Command $command
     * @return void
     */
    public function requeueCommand(Command $command)
    {
        $this->storage->enqueue($command);
        $this->logActivity('Command requeued: '.$command->getName());
        
//        $key = spl_object_hash($command);
//        if (isset($this->fetched[$key])) {
//            unset($this->fetched[$key]);
//            $this->storage->enqueue($command);
//            $this->logActivity('Command requeued: '.$command->getName());
//        } else {
//            $this->logWarning('Command '.$command->getName().' should be requeued but was not found');
//        }
    }
    
    /**
     * Publish command (send it to queue to execute by worker).
     *
     * @param Command $command
     * @return void
     */
    public function publishCommand(Command $command)
    {
        $this->storage->enqueue($command);
        $this->logActivity('Command published: '.$command->getName());
    }
    
    /**
     * Get name of queue.
     *
     * @return string
     */
    public function queueName()
    {
        return 'in_memory_tasks_queue';
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
     * @param string $level
     * @return void
     */
    private function log($message, $level)
    {
        $this->logger->log($level, $message);
    }
}