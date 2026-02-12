<?php

namespace App\MessageHandler;

use App\Message\RunProcessStepMessage;
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

		$step = $this->db->fetchAssociative('SELECT * FROM process_step 
             WHERE process_instance_id = ? AND step_name = ? 
             FOR UPDATE', [
				$message->processId,
				$message->stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			return;
		}

		// Идемпотентность: не даём повторно выполнять DONE
		if (in_array($step['status'], [
				'DONE',
				'FAILED'
		], true))
		{
			$this->db->rollBack();
			return;
		}

		// Если RUNNING — допускаем повтор только при retry Messenger (attempt == 0 или 1)
		if ($step['status'] === 'RUNNING' && $step['attempt'] > 1)
		{
			$this->db->rollBack();
			return;
		}

		// Переводим в RUNNING атомарно
		$this->db->executeStatement('UPDATE process_step 
             SET status = ?, attempt = attempt + 1, locked_at = NOW() 
             WHERE id = ?', [
				'RUNNING',
				$step['id']
		]);

		$this->db->commit();

		try
		{
			// === БИЗНЕС-ЛОГИКА ===
			switch ($message->stepName)
			{
				case 'prepare' :
					$this->callPrepare($message->processId);
					break;

				case 'dispatch' :
					$this->orchestrator->fanOut($message->processId, 'dispatch_fanout_1', [
							'call_api_a',
							'call_api_b',
							'generate_doc'
					]);
					break;

				case 'call_api_a' :
					$this->callApiA($message->processId);
					break;

				case 'call_api_b' :
					$this->callApiB($message->processId);
					break;

				case 'generate_doc' :
					$this->generateDocument($message->processId);
					break;

				case 'finalize' :
					$this->callFinalize($message->processId);
					break;

				default :
					throw new \LogicException('Unknown step: ' . $message->stepName);
			}

			// === УСПЕХ ===
			$this->orchestrator->markStepDone($message->processId, $message->stepName);

			if ($message->stepName === 'prepare')
			{
				$this->orchestrator->afterPrepare($message->processId);
			}

			if (!empty($step['join_group']))
			{
				$this->orchestrator->tryJoin($message->processId, $step['join_group'], 'finalize');
			}
		}
		catch ( \Throwable $e )
		{
			// === АВАРИЯ ===
			$this->orchestrator->markStepFailed($message->processId, $message->stepName, $e->getMessage());

			throw $e; // Messenger сам сделает retry / failure transport
		}
	}
	private function callPrepare(int $processId): void
	{
		sleep(1);
	}
	private function callApiA(int $processId): void
	{
		sleep(1);
	}
	private function callApiB(int $processId): void
	{
		sleep(1);
	}
	private function generateDocument(int $processId): void
	{
		sleep(1);
	}
	private function callFinalize(int $processId): void
	{
		sleep(1);
	}
}

