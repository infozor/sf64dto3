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
	public function fanOut(int $processId, string $joinGroup, array $steps): void
	{
		$this->db->beginTransaction();

		foreach ( $steps as $stepName )
		{
			$this->db->executeStatement('INSERT INTO process_step (process_instance_id, step_name, status, join_group)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (process_instance_id, step_name) DO NOTHING', [
					$processId,
					$stepName,
					'PENDING',
					$joinGroup
			]);

			$this->bus->dispatch(new RunProcessStepMessage($processId, $stepName));
		}

		$this->db->commit();
	}
	public function tryJoin(int $processId, string $joinGroup, string $nextStep): void
	{
		$this->db->beginTransaction();

		// Блокируем все шаги группы
		$rows = $this->db->fetchAllAssociative('SELECT id, status FROM process_step
         WHERE process_instance_id = ? AND join_group = ?
         FOR UPDATE', [
				$processId,
				$joinGroup
		]);

		foreach ( $rows as $row )
		{
			if ($row['status'] !== 'DONE')
			{
				$this->db->rollBack();
				return; // барьер ещё не пройден
			}
		}

		// Проверяем, что следующий шаг ещё не создан
		$exists = $this->db->fetchOne('SELECT 1 FROM process_step WHERE process_instance_id = ? AND step_name = ?', [
				$processId,
				$nextStep
		]);

		if (!$exists)
		{
			$this->db->insert('process_step', [
					'process_instance_id' => $processId,
					'step_name' => $nextStep,
					'status' => 'PENDING'
			]);

			$this->bus->dispatch(new RunProcessStepMessage($processId, $nextStep));
		}

		$this->db->commit();
	}
	public function markStepFailed(int $processId, string $stepName, string $error): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step WHERE process_instance_id = ? AND step_name = ? FOR UPDATE', [
				$processId,
				$stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			throw new \RuntimeException("process_step not found: {$processId} / {$stepName}");
		}

		// Идемпотентность: если шаг уже DONE — не затираем успешный результат
		if ($step['status'] === 'DONE')
		{
			$this->db->commit();
			return;
		}

		// Переводим в FAILED (или обновляем error, если уже FAILED)
		$this->db->executeStatement('UPDATE process_step
         SET status = ?, last_error = ?, finished_at = NOW()
         WHERE id = ?', [
				'FAILED',
				mb_substr($error, 0, 4000), // защита от переполнения поля
				$step['id']
		]);

		// (опционально) можно перевести весь процесс в FAILED
		$this->db->executeStatement('UPDATE process_instance
         SET status = ?
         WHERE id = ? AND status NOT IN (?, ?)', [
				'FAILED',
				$processId,
				'DONE',
				'FAILED'
		]);

		$this->db->commit();
	}
}
