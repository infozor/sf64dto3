<?php

// src/Service/CampaignsGet.php
namespace App\Service;

use GuzzleHttp\Client;

class CampaignsGet
{
	private string $token;
	private string $login;
	private bool $useSandbox;
	private Client $client;
	
	public function __construct(string $token, string $login, bool $useSandbox = false)
	{
		$this->token = $token;
		$this->login = $login;
		$this->useSandbox = $useSandbox;

		$baseUri = $useSandbox ? 'https://api-sandbox.direct.yandex.com/json/v5/' : 'https://api.direct.yandex.com/json/v5/';

		$this->client = new Client([
				'base_uri' => $baseUri,
				'headers' => [
						'Authorization' => 'Bearer ' . $this->token,
						'Client-Login' => $this->login,
						'Accept-Language' => 'ru',
						'Content-Type' => 'application/json; charset=utf-8'
				],
				'verify' => false
		]);
	}
	public function getActiveCampaigns(int $limit = 200, int $offset = 0): array
	{
		$body = [
				'method' => 'get',
				'params' => [
						'SelectionCriteria' => [
								// Берём все OFF (как у тебя было)
								'States' => [
										'OFF'
								]
						],
						'FieldNames' => [
								'Id',
								'Name',
								'State',
								'StartDate',
								'EndDate',
								'DailyBudget',
								'Type',
								'Status'
						],
						'Page' => [
								'Limit' => $limit,
								'Offset' => $offset
						]
				]
		];

		$response = $this->client->post('campaigns', [
				'json' => $body
		]);

		$data = json_decode($response->getBody()->getContents(), true);

		// API ошибка
		if (isset($data['error']))
		{
			return [
					'error' => $data['error']
			];
		}

		// Если данных нет
		if (!isset($data['result']['Campaigns']))
		{
			return [];
		}

		// Преобразуем как в старой версии
		return array_map(function ($c)
		{
			return [
					'id' => $c['Id'] ?? null,
					'name' => $c['Name'] ?? null,
					'state' => $c['State'] ?? null,
					'start_date' => $c['StartDate'] ?? null,
					'end_date' => $c['EndDate'] ?? null,
					'daily_budget' => isset($c['DailyBudget']['Amount']) ? $c['DailyBudget']['Amount'] / 1_000_000 : null,
					'type' => $c['Type'] ?? null,
					'status' => $c['Status'] ?? null
			];
		}, $data['result']['Campaigns']);
	}
}
