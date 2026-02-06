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
		//sleep(1); // имитация работы

		// 2. Выполняем бизнес-логику
		try
		{
			switch ($message->stepName)
			{
				case 'dispatch' :
					// точка ветвления
					$this->orchestrator->fanOut($message->processId, 'dispatch_fanout_1', [
							'call_api_a',
							'call_api_b',
							'generate_doc'
					]);
					break;

				case 'call_api_a' :
					if (!method_exists($this, 'callApiA'))
					{
						throw new \LogicException('callApiA() not implemented');
					}
					$this->callApiA($message->processId);
					break;

				case 'call_api_b' :
					if (!method_exists($this, 'callApiB'))
					{
						throw new \LogicException('callApiB() not implemented');
					}
					$this->callApiB($message->processId);
					break;

				case 'generate_doc' :
					if (!method_exists($this, 'generateDocument'))
					{
						throw new \LogicException('generateDocument() not implemented');
					}
					$this->generateDocument($message->processId);
					break;

				default :
					throw new \LogicException('Unknown step: ' . $message->stepName);
			}

			// 3. Если дошли сюда — шаг УСПЕШНО завершён
			$this->orchestrator->markStepDone($message->processId, $message->stepName);

			// 4. Если шаг участвует в fan-out группе — пытаемся пройти join
			if (!empty($step['join_group']))
			{
				$this->orchestrator->tryJoin($message->processId, $step['join_group'], 'finalize');
			}
		}
		catch ( \Throwable $e )
		{
			// 5. Любая ошибка = FAILED
			$this->orchestrator->markStepFailed($message->processId, $message->stepName, $e->getMessage());

			// 6. Пробрасываем исключение, чтобы Messenger сделал retry / failure transport
			throw $e;
		}

		$this->orchestrator->markStepDone($message->processId, $message->stepName);

		// Если шаг участвует в fan-out группе — пытаемся пройти join
		if ($step['join_group'])
		{
			$this->orchestrator->tryJoin($message->processId, $step['join_group'], 'finalize');
		}
	}
	private function callApiA(int $processId): void
	{
		// throw new \RuntimeException('API A failed (test)');
		sleep(1);
	}
	private function callApiB(int $processId): void
	{
		// throw new \RuntimeException('API B failed (test)');
		sleep(1);
	}
	private function generateDocument(int $processId): void
	{
		sleep(1);
	}
}
