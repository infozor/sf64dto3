<?php

// src/Process/ProcessOrchestrator.php
//namespace App\Process;
namespace App\ModuleProcess\Orchestrator;

use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\MessageBusInterface;
use App\Message\RunProcessStepMessage;

final class ProcessOrchestrator
{
	public function __construct(private Connection $db, private MessageBusInterface $bus)
	{
	}
	public function startProcess(string $processType, ?string $businessKey, array $payload, ?int $sourceJobId = null): int
	{
		$this->db->beginTransaction();

		$processId = $this->db->fetchOne('SELECT id FROM process_instance WHERE process_type = ? AND business_key = ? FOR UPDATE', [
				$processType,
				$businessKey
		]);

		if (!$processId)
		{
			$this->db->insert('process_instance', [
					'process_type' => $processType,
					'business_key' => $businessKey,
					'status' => 'RUNNING',
					'payload' => json_encode($payload),
					'source_job_id' => $sourceJobId,
					'started_at' => (new \DateTime())->format('Y-m-d H:i:s')
			]);
			$processId = ( int ) $this->db->lastInsertId();
		}

		$this->db->insert('process_step', [
				'process_instance_id' => $processId,
				'step_name' => 'prepare',
				'status' => 'PENDING'
		]);

		$this->db->commit();

		$this->bus->dispatch(new RunProcessStepMessage($processId, 'prepare'));

		return $processId;
	}
	public function markStepDone(int $processId, string $step): void
	{
		$this->db->executeStatement('UPDATE process_step SET status = ? WHERE process_instance_id = ? AND step_name = ?', [
				'DONE',
				$processId,
				$step
		]);

		$next = match ($step) {
				'prepare' => 'dispatch',
				'dispatch' => 'finalize',
				default => null
		};

		if ($next)
		{
			$this->db->insert('process_step', [
					'process_instance_id' => $processId,
					'step_name' => $next,
					'status' => 'PENDING'
			]);
			$this->bus->dispatch(new RunProcessStepMessage($processId, $next));
		}
		else
		{
			$this->db->executeStatement('UPDATE process_instance SET status = ?, finished_at = NOW() WHERE id = ?', [
					'COMPLETED',
					$processId
			]);
		}
	}
}
