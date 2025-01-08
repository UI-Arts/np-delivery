<?php

namespace UIArts\NPDelivery\Console\Commands;

use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use JsonMachine\Items;
use UIArts\NPDelivery\Services\NPIService;
use UIArts\NPDelivery\Traits\HasCountryCodes;

class NPISave extends Command
{
    use HasCountryCodes;
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'npi:save';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Save International - Country, Settlement, Street';

    protected \Elasticsearch\Client $esClient;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct(ClientBuilder $esClient)
    {
        $this->esClient = $esClient->create()->setHosts(config('np-delivery.elasticHost'))->build();
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return void
     * @throws GuzzleException
     */
    public function handle():void
    {
        ini_set('memory_limit', '1024M');

        $this->info('Starting the process of fetching and processing NP data...');

        $versions = $this->send('GET', 'https://api.novapost.com/divisions/versions', ['Accept-Language' => 'en']);
        $tempFile = tempnam(sys_get_temp_dir(), 'base_version') . '.gz';

        $this->comment('Downloading and saving data to a temporary file...');
        file_put_contents($tempFile, fopen($versions['base_version']['url'], 'r'));

        $gz = gzopen($tempFile, 'rb');
        if (!$gz) {
            $this->error('Failed to open the gzipped file.');
            return;
        }

        $newCountriesIndex = NPIService::COUNTRIES_INDEX . '_' . time();
        $newCitiesIndex = NPIService::CITIES_INDEX . '_' . time();
        $newDivisionsIndex = NPIService::DIVISIONS_INDEX . '_' . time();

        $this->createMapping($newCountriesIndex, [
            'code' => ['type' => 'keyword'],
            'name' => ['type' => 'keyword']
        ]);

        $this->createMapping($newCitiesIndex, [
            'id' => ['type' => 'keyword'],
            'name' => ['type' => 'keyword'],
            'countryCode' => ['type' => 'keyword']
        ]);

        $this->createMapping($newDivisionsIndex, [
            'id' => ['type' => 'keyword'],
            'name' => ['type' => 'keyword'],
            'street' => ['type' => 'text'],
            'building' => ['type' => 'text'],
            'zipcode' => ['type' => 'text'],
            'address' => ['type' => 'text'],
            'externalId' => ['type' => 'text'],
            'countryCode' => ['type' => 'keyword'],
            'cityId' => ['type' => 'text'],
        ]);

        $this->info('Processing the JSON stream from the compressed file...');

        $processedCountries = [];
        $processedCities = [];
        $dataBulk = [];
        $totalDivisions = 0;
        $jsonStream = Items::fromStream($gz, ['pointer' => '/items']);

        foreach ($jsonStream as $division) {
            $countryCode = Str::lower($division->countryCode);
            if ($countryCode === 'ua') {
                continue; //skip local divisions
            }

            $cityId = $division->settlement->id;
            $divisionId = $division->id;

            if (!isset($processedCountries[$countryCode])) {
                $dataBulk[] = ['index' => ['_index' => $newCountriesIndex, '_id' => $countryCode]];
                $dataBulk[] = ['code' => $countryCode, 'name' => $this->getCountryByCode($division->countryCode),];
                $processedCountries[$countryCode] = true;
            }

            if (!isset($processedCities[$cityId])) {
                $dataBulk[] = ['index' => ['_index' => $newCitiesIndex, '_id' => $cityId]];
                $dataBulk[] = ['id' => $cityId, 'name' => $division->addressParts->city, 'countryCode' => $countryCode];
                $processedCities[$cityId] = true;
            }

            $dataBulk[] = ['index' => ['_index' => $newDivisionsIndex, '_id' => $divisionId]];
            $dataBulk[] = [
                'id' => $divisionId,
                'name' => $division->name,
                'street' => $division->addressParts->street,
                'building' => $division->addressParts->building,
                'zipcode' => $division->addressParts->postCode,
                'address' => $division->address,
                'externalId' => $division->externalId,
                'cityId' => $cityId,
                'countryCode' => $countryCode,
            ];

            $totalDivisions++;
            if ($totalDivisions % 1000 === 0) {
                $this->bulkIndex($dataBulk);
                $dataBulk = [];
                $this->output->write("\r" . "Processed $totalDivisions divisions...", false);
            }
        }

        if (!empty($dataBulk)) {
            $this->bulkIndex($dataBulk);
            $this->output->write("\r" . "Processed $totalDivisions divisions...", false);
        }

        $this->switchIndexes(NPIService::COUNTRIES_INDEX, $newCountriesIndex);
        $this->switchIndexes(NPIService::CITIES_INDEX, $newCitiesIndex);
        $this->switchIndexes(NPIService::DIVISIONS_INDEX, $newDivisionsIndex);

        gzclose($gz);
        unlink($tempFile);

        $this->info("\n Process completed successfully.");
    }

    /**
     * @param array $bulkData
     * @return void
     */
    private function bulkIndex(array $bulkData): void
    {
        $response = $this->esClient->bulk(['body' => $bulkData]);

        if (isset($response['errors']) && $response['errors']) {
            foreach ($response['items'] as $item) {
                if (isset($item['index']['error'])) {
                    $this->error("Error indexing document: " . json_encode($item['index']['error']));
                }
            }
        }
    }

    /**
     * @param string $indexName
     * @param array $mapping
     * @return void
     */
    protected function createMapping(string $indexName, array $mapping = []): void
    {
        $params = ['index' => $indexName];
        if (!empty($mapping)) {
            $params['body']['mappings']['properties'] = $mapping;
        }
        $params['body']['settings']['index']['max_result_window'] = 100000;
        $this->esClient->indices()->create($params);
    }

    /**
     * @param string $index
     * @param string $newIndex
     * @return void
     */
    public function switchIndexes(string $index, string $newIndex): void
    {
        $client = $this->esClient->indices();
        $aliasActions[] = [
            'add' => [
                'index' => $newIndex,
                'alias' => $index
            ]
        ];
        $deletedIndices = [];

        $oldIndices = $this->getIndexesByAlias($index);
        foreach ($oldIndices as $oldIndex) {
            if ($oldIndex != $newIndex) {
                $deletedIndices[] = $oldIndex;
                $aliasActions[] = [
                    'remove' => [
                        'index' => $oldIndex,
                        'alias' => $index,
                    ]
                ];
            }
        }
        $client->updateAliases(['body' => ['actions' => $aliasActions]]);
        foreach ($deletedIndices as $deletedIndex) {
            $client->delete(['index' => $deletedIndex]);
        }
    }

    /**
     * @param string $indexAlias
     * @return array
     */
    public function getIndexesByAlias(string $indexAlias): array
    {
        $indices = [];
        try {
            $indices = $this->esClient->indices()->getAlias(['index' => $indexAlias . '*']);
        } catch (Missing404Exception $e) {
        }
        return array_keys($indices);
    }

    /**
     * @param string $method
     * @param string $url
     * @param array $headers
     * @return mixed|null
     * @throws GuzzleException
     */
    private function send(string $method, string $url, array $headers = [])
    {
        $client = new Client();
        try {
            $response = $client->request($method, $url, [
                'headers' => $headers,
                'timeout' => 10,
                'allow_redirects'  => true,
                'verify' => false,
            ]);
            return json_decode($response->getBody(), true);
        } catch (\Exception $e) {
            $errorMessage = $e->getMessage();
            Log::warning("Send NovaPostInternationalService: method - {$method}, url - {$url}, message - {$errorMessage}");
            return null;
        }
    }
}