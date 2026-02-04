<?php

// src/Command/RunSchedulerCommand.php

namespace App\ModuleProcess\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;



#[AsCommand(name: 'app:scheduler:run')]
final class RunSchedulerCommand extends Command
{
	public function __construct(private Connection $db, private ProcessOrchestrator $orchestrator)
	{
		parent::__construct();
	}
	protected function execute(InputInterface $input, OutputInterface $output): int
	{
		$jobs = $this->db->fetchAllAssociative('SELECT * FROM scheduled_jobs
             WHERE status = ? AND scheduled_at <= NOW()
             ORDER BY scheduled_at
             FOR UPDATE SKIP LOCKED
             LIMIT 10', [
				'NEW'
		]);

		foreach ( $jobs as $job )
		{
			$this->db->beginTransaction();

			$this->db->executeStatement('UPDATE scheduled_jobs SET status = ?, locked_at = NOW() WHERE id = ?', [
					'LOCKED',
					$job['id']
			]);

			$this->orchestrator->startProcess($job['process_type'], $job['business_key'], json_decode($job['payload'], true), (int) $job['id']);

			$this->db->executeStatement('UPDATE scheduled_jobs SET status = ? WHERE id = ?', [
					'DONE',
					$job['id']
			]);

			$this->db->commit();
		}

		return Command::SUCCESS;
	}
}
