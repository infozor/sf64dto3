<?php

// src/Service/CampaignGetDirect.php
namespace App\Service;

use GuzzleHttp\Client;

class CampaignGetDirect
{
	private $client;
	public function __construct(string $token, string $login, bool $useSandbox = false)
	{
		$baseUri = $useSandbox ? 'https://api-sandbox.direct.yandex.com/json/v501/' : 'https://api.direct.yandex.com/json/v501/';

		$this->client = new Client([
				'base_uri' => $baseUri,
				'headers' => [
						'Authorization' => 'Bearer ' . $token,
						'Accept-Language' => 'ru',
						'Client-Login' => $login,
						'Content-Type' => 'application/json; charset=utf-8'
				],
				'verify' => false // пока SSL проверку отключаем
		]);
	}


	public function getCampaign(int $campaignId): array
	{
		$body = [
				'method' => 'get',
				'params' => [
						'SelectionCriteria' => [
								'Ids' => [
										$campaignId
								]
						],
						'FieldNames' => [
								'Id',
								'Name',
								'ClientInfo',
								'StartDate',
								'EndDate',
								'Type',
								'Status',
								'State',
								'StatusPayment',
								'StatusClarification',
								'SourceId',
								'Currency',
								'Funds',
								'DailyBudget',
								'Notification',
								'NegativeKeywords',
								'BlockedIps',
								'ExcludedSites',
								'TimeZone',
								'Statistics'
						],
						'TextCampaignFieldNames' => [
								'BiddingStrategy',
								'Settings'
						],
						'DynamicTextCampaignFieldNames' => [
								'BiddingStrategy',
								'Settings'
						],
						'CpmBannerCampaignFieldNames' => [
								'BiddingStrategy'
						],
						'SmartCampaignFieldNames' => [
								'BiddingStrategy'
						]
				]
		];

		$response = $this->client->post('campaigns', [
				'json' => $body
		]);

		return json_decode($response->getBody()->getContents(), true);
	}
}

