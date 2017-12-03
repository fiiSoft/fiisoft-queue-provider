<?php

namespace FiiSoft\Queue\InMemory;

use FiiSoft\Logger\SmartLogger;
use FiiSoft\TasksQueue\Command;
use FiiSoft\TasksQueue\CommandQueue;
use LogicException;
use SplQueue;

final class InstantCommandQueue implements CommandQueue
{
    /** @var SmartLogger */
    private $logger;
    
    /** @var SplQueue */
    private $storage;
    
    /**
     * @param SmartLogger $logger
     */
    public function __construct(SmartLogger $logger)
    {
        $this->logger = $logger;
        $this->logger->setPrefix('[Q] ')->setContext(['source' => 'queue']);
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
     * @param string $level
     * @return void
     */
    private function log($message, $level)
    {
        $this->logger->log($level, $message);
    }
}