<?php

// src/MessageHandler/RunProcessStepHandler.php
namespace App\MessageHandler;

use App\Message\RunProcessStepMessage;
//use App\Process\ProcessOrchestrator;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;



use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class RunProcessStepHandler
{
	public function __construct(private Connection $db, private ProcessOrchestrator $orchestrator)
	{
	}
	public function __invoke(RunProcessStepMessage $message): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step WHERE process_instance_id = ? AND step_name = ? FOR UPDATE', [
				$message->processId,
				$message->stepName
		]);

		if (!$step || $step['status'] !== 'PENDING')
		{
			$this->db->rollBack();
			return;
		}

		$this->db->executeStatement('UPDATE process_step SET status = ?, attempt = attempt + 1 WHERE id = ?', [
				'RUNNING',
				$step['id']
		]);

		$this->db->commit();

		// Бизнес-логика шага
		sleep(1); // имитация работы

		$this->orchestrator->markStepDone($message->processId, $message->stepName);
	}
}
