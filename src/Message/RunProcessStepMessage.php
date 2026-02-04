<?php

// src/Message/RunProcessStepMessage.php
namespace App\Message;

final class RunProcessStepMessage
{
	public function __construct(public readonly int $processId, public readonly string $stepName, public readonly ?int $sourceJobId = null)
	{
	}
}
