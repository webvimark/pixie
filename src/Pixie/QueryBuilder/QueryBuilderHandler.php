<?php 
namespace Pixie\QueryBuilder;

use PDO;
use Pixie\Connection;
use Pixie\Exception;

class QueryBuilderHandler
{
    const RECONNECT_ATTEMPTS = 3;

    protected $reconnect_attempt = 0;

    /**
     * @var \Viocon\Container
     */
    protected $container;

    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @var array
     */
    protected $statements = array();

    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var null|PDOStatement
     */
    protected $pdoStatement = null;

    /**
     * @var null|string
     */
    protected $tablePrefix = null;

    /**
     * @var \Pixie\QueryBuilder\Adapters\BaseAdapter
     */
    protected $adapterInstance;

    /**
     * The PDO fetch parameters to use
     *
     * @var array
     */
    protected $fetchParameters = array(PDO::FETCH_ASSOC);

    /**
     * Cache class with set() and get() methods
     *
     * @var object|null
     */
    protected $cacheHandler;
    /**
     * Cache time in seconds. 0 - no cache
     *
     * @var int
     */
    protected $cacheTtl = 0;
    /**
     * If not set then sha1 of the raw SQL will be used
     *
     * @var string
     */
    protected $cacheKey;

    /**
     * If get() method should dump raw SQL
     *
     * @var boolean
     */
    protected $dump = false;

    /**
     * Set of closure functions that will perform array_map() on result one by one
     *
     * @var array
     */
    protected $mapFunctions = [];

    /**
     * @see $this->withMany() and $this->withManyVia()
     * @var array
     */
    protected $withs = [];

    /**
     * Assigned in $this->query()
     * [
     *      'sql' => '...', 
     *      'bindings' => [],
     *      'executionTime' => '',
     * ]
     *
     * @var array
     */
    protected $_query;

    /**
     * @param null|\Pixie\Connection $connection
     *
     * @param int $fetchMode
     * @throws Exception
     */
    public function __construct(Connection $connection = null, $fetchMode = PDO::FETCH_ASSOC)
    {
        if (is_null($connection)) {
            if (!$connection = Connection::getStoredConnection()) {
                throw new Exception('No database connection found.', 1);
            }
        }

        $this->connection = $connection;
        $this->container = $this->connection->getContainer();
        $this->pdo = $this->connection->getPdoInstance();
        $this->adapter = $this->connection->getAdapter();
        $this->adapterConfig = $this->connection->getAdapterConfig();

        $this->setFetchMode($fetchMode);

        if (isset($this->adapterConfig['prefix'])) {
            $this->tablePrefix = $this->adapterConfig['prefix'];
        }

        // Query builder adapter instance
        $this->adapterInstance = $this->container->build(
            '\\Pixie\\QueryBuilder\\Adapters\\' . ucfirst($this->adapter),
            array($this->connection)
        );

        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Set the fetch mode
     *
     * @param $mode
     * @return $this
     */
    public function setFetchMode($mode)
    {
        $this->fetchParameters = func_get_args();
        return $this;
    }

    /**
     * Fetch query results as object of specified type
     *
     * @param $className
     * @param array $constructorArgs
     * @return QueryBuilderHandler
     */
    public function asObject($className, $constructorArgs = array())
    {
        return $this->setFetchMode(PDO::FETCH_CLASS, $className, $constructorArgs);
    }

    /**
     * @param null|\Pixie\Connection $connection
     * @return QueryBuilderHandler
     * @throws Exception
     */
    public function newQuery(Connection $connection = null)
    {
        if (is_null($connection)) {
            $connection = $this->connection;
        }

        return new static($connection, $this->getFetchMode());
    }

    /**
     * Return prepared final SQL. For debug purposes only!
     *
     * @return string
     */
    public function getSql()
    {
        if ($this->_query) {
            $queryObject = new QueryObject($this->_query['sql'], $this->_query['bindings'], $this->pdo);
        } else {
            $queryObject = $this->getQuery();
        }

        return $queryObject->getRawSql();
    }

    /**
     * @param       $sql
     * @param array $bindings
     *
     * @return $this
     */
    public function query($sql, $bindings = array())
    {
        list($this->pdoStatement, $executionTime) = $this->statement($sql, $bindings);
        $this->_query = compact('sql', 'bindings', 'executionTime');

        return $this;
    }

    /**
     * @param       $sql
     * @param array $bindings
     *
     * @return array PDOStatement and execution time as float
     */
    public function statement($sql, $bindings = array())
    {
        $start = microtime(true);

        $pdoStatement = $this->safeExecute($sql, $bindings);

        return array($pdoStatement, microtime(true) - $start);
    }

    /**
     * Try to execute PDOStatement and reconnect to the server if timeout occurs
     * Used by $this->statement()
     *
     * @param string $sql
     * @param array $bindings
     * @return \PDOStatement
     */
    protected function safeExecute($sql, $bindings)
    {
        try {
            $pdoStatement = $this->pdo->prepare($sql);
            foreach ($bindings as $key => $value) {
                $pdoStatement->bindValue(
                    is_int($key) ? $key + 1 : $key,
                    $value,
                    is_int($value) || is_bool($value) ? PDO::PARAM_INT : PDO::PARAM_STR
                );
            }

            $pdoStatement->execute();

            return $pdoStatement;
        } catch (\Exception $e) {
            $shouldTryToReconnect = false;
            $errorMessages = ['server has gone away', 'Error while sending QUERY packet'];
            foreach ($errorMessages as $errorMessage) {
                if (stripos($e->getMessage(), $errorMessage) !== false) {
                    $shouldTryToReconnect = true;
                    break;
                }
            }
            if ($shouldTryToReconnect) {
                if ($this->reconnect_attempt++ < self::RECONNECT_ATTEMPTS) {
                    $this->connection->connect();
                    $this->pdo = $this->connection->getPdoInstance();
                    $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

                    return $this->safeExecute($sql, $bindings);
                }
            }

            throw $e;
        }
    }

    /**
     * Cache handler class should have following methods:
     *  - get($key) - return stored value or FALSE if value is not exists or expired
     *  - set($key, $value, $ttl) - store value for $ttl seconds
     *
     * @param class $cacheHandler
     * @return $this
     */
    public function setCacheHandler($cacheHandler)
    {
        if (!method_exists($cacheHandler, 'get') || !method_exists($cacheHandler, 'set')) {
            throw new \InvalidArgumentException('Cache handler should have get() and set() methods');
        }

        $this->cacheHandler = $cacheHandler;
        return $this;
    }

    /**
     * Cache time in seconds. 0 - no cache
     *
     * @param integer $ttl
     * @param string $cacheKey - if not set then sha1 of the raw SQL will be used
     * @return $this
     */
    public function cache($ttl = 3600, $cacheKey = null)
    {
        if (!is_int($ttl) || $ttl < 0) {
            throw new \InvalidArgumentException('Cache ttl should be positive integer or 0');
        }
        $this->cacheTtl = $ttl;
        $this->cacheKey = $cacheKey;
        return $this;
    }

    /**
     * array_map() function that will be performed on result
     * 
     * @param callable $mapFunction
     * @return $this
     */
    public function map(callable $mapFunction)
    {
        $this->mapFunctions[] = $mapFunction;
        return $this;
    }

    /**
     * Add 1-1 related data from "external_table"
     *
     * @param null|callable $func
     * @param string $external_table
     * @param string $original_table_id
     * @param string $external_table_id
     * @param string $name
     * @return $this
     */
    public function withOne($func, $external_table, $original_table_id, $external_table_id = 'id', $name = null)
    {
        if ($name === null) {
            $name = $external_table;
        }

        $this->withs[$name] = array_merge(
            ['type' => __FUNCTION__],
            compact(
                'func',
                'name',
                'external_table',
                'external_table_id',
                'original_table_id'
            )
        );

        return $this;
    }

    /**
     * Add 1-N related data from "external_table"
     *
     * @param null|callable $func
     * @param string $external_table
     * @param string $external_table_id
     * @param string $original_table_id
     * @param string $name
     * @return $this
     */
    public function withMany($func, $external_table, $external_table_id, $original_table_id = 'id', $name = null)
    {
        if ($name === null) {
            $name = $external_table;
        }

        $this->withs[$name] = array_merge(
            ['type' => __FUNCTION__],
            compact(
                'func',
                'name',
                'external_table',
                'external_table_id',
                'original_table_id'
            )
        );

        return $this;
    }

    /**
     * Add 1-N related data from "external_table" connnected from "via_table"
     * 
     * By default we execute 2 additional quries instead 1 additional query with inner join
     * you can change it with $joinInsteadSelect param
     *
     * @param null|callable $func
     * @param string $external_table
     * @param string $via_table
     * @param string $via_table_original_id
     * @param string $via_table_external_id
     * @param string $external_table_id
     * @param string $original_table_id
     * @param string $name
     * @param bool $joinInsteadSelect
     * @return $this
     */
    public function withManyVia(
        $func,
        $external_table,
        $via_table,
        $via_table_original_id,
        $via_table_external_id,
        $external_table_id = 'id',
        $original_table_id = 'id',
        $name = null,
        $joinInsteadSelect = false
    ) {
        if ($name === null) {
            $name = $external_table;
        }

        $this->withs[$name] = array_merge(
            ['type' => __FUNCTION__],
            compact(
                'func',
                'name',
                'joinInsteadSelect',
                'external_table',
                'via_table',
                'via_table_original_id',
                'via_table_external_id',
                'external_table_id',
                'original_table_id'
            )
        );

        return $this;
    }

    /**
     * If get() method should dump raw SQL
     * 
     * @param boolean $dump
     * @return $this
     */
    public function dump($dump = true)
    {
        $this->dump = $dump;
        return $this;
    }

    /**
     * Note - be careful with the left and right joins (because they can add more rows)
     * 
     * Respond with array containing following elements
     * [
     *      'total' => 183, // total records
     *      'perPage' => 20,
     *      'currentPage' => 5,
     *      'items' => [...], // array of results
     * ]
     *
     * @param integer $currentPage
     * @param integer $perPage
     * @param bool $lateLookup - https://stackoverflow.com/a/4502426 can give huge performance boost for relatively simple queries
     * @param false|int $preDefinedTotal - set some number if you do not want to execute "$this->count()"
     * @return array
     */
    public function paginate($currentPage, $perPage = 20, $lateLookup = false, $preDefinedTotal = false)
    {
        $currentPage = (int)$currentPage >= 1 ? (int)$currentPage : 1;
        $perPage = (int)$perPage >= 1 ? (int)$perPage : 1;

        $this->limit($perPage)->offset(($currentPage - 1) * $perPage);

        $total = is_numeric($preDefinedTotal) ? (int)abs($preDefinedTotal) : $this->count();

        if ($lateLookup) {
            $queryString = $this->getQuery()->getRawSql();
            $subQuery = preg_replace('/^select (.*?) from/i', 'select id as __pagination_id_placehodler from', $queryString);

            preg_match('/^select .* from (.*?) /i', $queryString, $matches);
            $mainTable = '`' . trim($matches[1], '`') . '`';

            preg_match('/^select (.*?) from /i', $queryString, $matches);
            $mainSelect = $matches[0];

            $sql = "{$mainSelect} ({$subQuery}) as __pagination_table_placehodler JOIN {$mainTable} "
                . " ON {$mainTable}.id = __pagination_table_placehodler.__pagination_id_placehodler";

            $this->statements = [];
            $items = $this->query($sql)->get();
        } else {
            $items = $this->get();
        }

        return [
            'perPage' => $perPage,
            'currentPage' => $currentPage,
            'total' => $total,
            'items' => $items,
        ];
    }

    /**
     * Iterate over "withs" and add related datasets to the result
     *
     * @param array|null $result
     * @return array|null
     */
    protected function performWith($result)
    {
        if (!$result || !$this->withs) {
            return $result;
        }

        foreach ($this->withs as $name => $params) {
            $originalTableResultIds = array_filter(array_unique(array_column($result, $params['original_table_id'])));
            $with = [];

            if ($originalTableResultIds) {
                if ($params['type'] === 'withManyVia') {
                    if ($params['joinInsteadSelect']) {
                        $qb = $this->table($params['external_table'])
                            ->innerJoin(
                                $params['via_table'],
                                $params['via_table'] . '.' . $params['via_table_external_id'],
                                '=',
                                $params['external_table'] . '.' . $params['external_table_id']
                            )
                            ->select($params['external_table'] . '.*')
                            ->select([$params['via_table'] . '.' . $params['via_table_original_id'] => '___placeholder'])
                            ->whereIn($params['via_table'] . '.' . $params['via_table_original_id'], $originalTableResultIds);
                    } else {
                        $withManyViaPlaceholder = $this->table($params['via_table'])
                            ->select($params['via_table'] . '.' . $params['via_table_external_id'])
                            ->select([$params['via_table'] . '.' . $params['via_table_original_id'] => '___placeholder'])
                            ->whereIn($params['via_table_original_id'], $originalTableResultIds)
                            ->get();

                        $qb = $this->table($params['external_table'])
                            ->whereIn(
                                $params['external_table_id'],
                                array_unique(array_column($withManyViaPlaceholder, $params['via_table_external_id']))
                            );
                    }

                    if ($params['func']) {
                        $qb = $params['func']($qb);
                    }
                    $tmp = $qb->get();

                    $with = [];
                    $withManyViaSorted = [];
                    foreach ($tmp as $tmp_with) {
                        $with[$tmp_with[$params['external_table_id']]] = $tmp_with;

                        // Sort relation table records in correlation with external table
                        foreach ($withManyViaPlaceholder as $tmp_withManyViaPl) {
                            if ($tmp_withManyViaPl[$params['via_table_external_id']] == $tmp_with[$params['external_table_id']]) {
                                $withManyViaSorted[] = $tmp_withManyViaPl;
                            }
                        }
                    }
                    $withManyViaPlaceholder = null;
                    $tmp = null;
                } elseif (in_array($params['type'], ['withOne', 'withMany'])) {
                    $qb = $this->table($params['external_table'])
                        ->select($params['external_table'] . '.*')
                        ->select([$params['external_table'] . '.' . $params['external_table_id'] => '___placeholder'])
                        ->whereIn($params['external_table'] . '.' . $params['external_table_id'], $originalTableResultIds);

                    if ($params['func']) {
                        $qb = $params['func']($qb);
                    }
                    $with = $qb->get();
                }
            }

            foreach ($result as &$item) {
                $item[$params['name']] = [];

                if ($params['type'] === 'withManyVia' && !$params['joinInsteadSelect']) {
                    foreach ($withManyViaSorted as $tmp) {
                        if ($item[$params['original_table_id']] == $tmp['___placeholder']) {
                            if (isset($with[$tmp[$params['via_table_external_id']]])) {
                                $item[$params['name']][] = $with[$tmp[$params['via_table_external_id']]];
                            }
                        }
                    }
                } else {
                    foreach ($with as $val) {
                        if ($item[$params['original_table_id']] == $val['___placeholder']) {
                            unset($val['___placeholder']);
                            if ($params['type'] === 'withOne') {
                                $item[$params['name']] = $val;
                            } else {
                                $item[$params['name']][] = $val;
                            }
                        }
                    }
                }
            }
        }
        $with = null;
        $withManyViaPlaceholder = null;

        return $result;
    }

    /**
     * Helper function for get() to implement array_map() on results
     * if $this->mapFunction is set
     *
     * @param array $result
     * @return array
     */
    protected function mapResults($result)
    {
        foreach ($this->mapFunctions as $function) {
            $result = array_map($function, $result);
        }

        return $result;
    }

    /**
     * Get 1-dimmenstional array with values from the first selected column
     *
     * @return array
     */
    public function getColumn($select = null)
    {
        if ($select) {
            unset($this->statements['selects']);
            $this->select($select);
        }
        return $this->setFetchMode(\PDO::FETCH_COLUMN)->get();
    }

    /**
     * Get scalar value from the first selected column first found row
     *
     * @return string|null
     */
    public function getScalar($select = null)
    {
        if ($select) {
            unset($this->statements['selects']);
            $this->select($select);
        }
        return $this->setFetchMode(\PDO::FETCH_COLUMN)->first();
    }

    /**
     * Return indexed array. Usefull for dropdowns
     *
     * @param string $indexField
     * @param string $valueField
     * @return array
     */
    public function pluck($indexField, $valueField)
    {
        unset($this->statements['selects']);
        $items = $this->select([$indexField, $valueField])->get();
        $result = [];
        foreach ($items as $item) {
            $result[$item[$indexField]] = $item[$valueField];
        }
        $items = null;

        return $result;
    }

    /**
     * Get items buy chunks - $rows and work with them - $function
     * Iteration will stop if $function will return false
     *
     * @param int $indexField
     * @param callable $valueField
     */
    public function chunk($rows, callable $function)
    {
        $i = 0;
        while ($items = $this->limit($rows)->offset($rows * $i)->get()) {
            $i++;
            if ($function($items, $i) === false) {
                break;
            }
        }
        $items = null;
    }

    /**
     * Get all rows. Wrapper for $this->getResult()
     *
     * @return array|\stdClass|null
     */
    public function get()
    {
        if ($this->dump) {
            echo $this->getSql();
            die;
        }

        if ($this->cacheTtl > 0 && $this->cacheHandler) {
            $cacheKey = $this->cacheKey ?: sha1($this->getSql());
            $result = $this->cacheHandler->get($cacheKey);

            if ($result === false) {
                $result = $this->mapResults($this->performWith($this->getResult()));

                $this->cacheHandler->set($cacheKey, $result, $this->cacheTtl);
            }

            return $result;
        }
        return $this->mapResults($this->performWith($this->getResult()));
    }

    /**
     * Get all rows. Used in $this->get()
     *
     * @return \stdClass|array
     * @throws Exception
     */
    protected function getResult()
    {
        $eventResult = $this->fireEvents('before-select');
        if (!is_null($eventResult)) {
            return $eventResult;
        };

        $executionTime = 0;
        if (is_null($this->pdoStatement)) {
            $queryObject = $this->getQuery('select');
            list($this->pdoStatement, $executionTime) = $this->statement(
                $queryObject->getSql(),
                $queryObject->getBindings()
            );
        }

        $start = microtime(true);
        $result = call_user_func_array(array($this->pdoStatement, 'fetchAll'), $this->fetchParameters);
        $executionTime += microtime(true) - $start;
        if ($this->_query) {
            $executionTime += $this->_query['executionTime'];
        }
        $this->pdoStatement = null;
        $this->fireEvents('after-select', $result, $executionTime);
        return $result;
    }

    /**
     * Get first row
     *
     * @return \stdClass|null
     */
    public function first()
    {
        $this->limit(1);
        $result = $this->get();
        return empty($result) ? null : $result[0];
    }

    /**
     * @param        $value
     * @param string $fieldName
     *
     * @return null|\stdClass
     */
    public function findAll($fieldName, $value)
    {
        $this->where($fieldName, '=', $value);
        return $this->get();
    }

    /**
     * @param        $value
     * @param string $fieldName
     *
     * @return null|\stdClass
     */
    public function find($value, $fieldName = 'id')
    {
        $this->where($fieldName, '=', $value);
        return $this->first();
    }

    /**
     * Get count of rows
     *
     * @return int
     */
    public function count()
    {
        $thisQB = clone $this;
        $thisQB->withs = [];
        $thisQB->mapFunctions = [];

        // Get the current statements
        $originalStatements = $thisQB->statements;

        unset($thisQB->statements['orderBys']);
        unset($thisQB->statements['limit']);
        unset($thisQB->statements['offset']);

        if (isset($thisQB->statements['groupBys']) || isset($thisQB->statements['havings'])) {
            $count = $thisQB
                ->query('SELECT COUNT(*) as cnt FROM (' . $thisQB->getSql() . ') as cnt_table')
                ->getScalar('cnt');
        } else {
            $count = $thisQB->aggregate('count');
        }

        $thisQB->statements = $originalStatements;

        return $count;
    }

    /**
     * @param $type
     *
     * @return int
     */
    protected function aggregate($type)
    {
        // Get the current selects
        $mainSelects = isset($this->statements['selects']) ? $this->statements['selects'] : null;
        // Replace select with a scalar value like `count`
        $this->statements['selects'] = array($this->raw($type . '(*) as field'));
        $row = $this->get();

        // Set the select as it was
        if ($mainSelects) {
            $this->statements['selects'] = $mainSelects;
        } else {
            unset($this->statements['selects']);
        }

        if (is_array($row[0])) {
            return (int)$row[0]['field'];
        } elseif (is_object($row[0])) {
            return (int)$row[0]->field;
        }

        return 0;
    }

    /**
     * @param string $type
     * @param array $dataToBePassed
     *
     * @return mixed
     * @throws Exception
     */
    public function getQuery($type = 'select', $dataToBePassed = array())
    {
        $allowedTypes = array('select', 'insert', 'insertignore', 'replace', 'delete', 'update', 'criteriaonly');
        if (!in_array(strtolower($type), $allowedTypes)) {
            throw new Exception($type . ' is not a known type.', 2);
        }

        $queryArr = $this->adapterInstance->$type($this->statements, $dataToBePassed);

        return $this->container->build(
            '\\Pixie\\QueryBuilder\\QueryObject',
            array($queryArr['sql'], $queryArr['bindings'], $this->pdo)
        );
    }

    /**
     * @param QueryBuilderHandler $queryBuilder
     * @param null $alias
     *
     * @return Raw
     */
    public function subQuery(QueryBuilderHandler $queryBuilder, $alias = null)
    {
        $sql = '(' . $queryBuilder->getQuery()->getRawSql() . ')';
        if ($alias) {
            $sql = $sql . ' as ' . $alias;
        }

        return $queryBuilder->raw($sql);
    }

    /**
     * Ensure that conjunction table has only $relatedIds for $mainId
     *
     * @param string $mainIdName
     * @param string $mainId
     * @param string $relatedIdName
     * @param array $relatedIds
     */
    public function saveRelated($mainIdName, $mainId, $relatedIdName, array $relatedIds)
    {
        $existingRelatedIds = $this->where($mainIdName, $mainId)->getColumn($relatedIdName);

        $relatedIdsToDelete = array_diff($existingRelatedIds, $relatedIds);
        if ($relatedIdsToDelete) {
            $this->where($mainIdName, $mainId)->whereIn($relatedIdName, $relatedIdsToDelete)->delete();
        }

        $relatedIdsToAdd = array_diff($relatedIds, $existingRelatedIds);
        if ($relatedIdsToAdd) {
            $data = [];
            foreach ($relatedIdsToAdd as $idToAdd) {
                $data[] = [
                    $mainIdName => $mainId,
                    $relatedIdName => $idToAdd,
                ];
            }
            $this->insert($data);
        }
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    private function doInsert($data, $type)
    {
        $eventResult = $this->fireEvents('before-insert');
        if (!is_null($eventResult)) {
            return $eventResult;
        }

        // If first value is not an array
        // Its not a batch insert
        if (!is_array(current($data))) {
            $queryObject = $this->getQuery($type, $data);

            list($result, $executionTime) = $this->statement($queryObject->getSql(), $queryObject->getBindings());

            $return = $result->rowCount() === 1 ? $this->pdo->lastInsertId() : null;
        } else {
            // Its a batch insert
            $return = array();
            $executionTime = 0;
            foreach ($data as $subData) {
                $queryObject = $this->getQuery($type, $subData);

                list($result, $time) = $this->statement($queryObject->getSql(), $queryObject->getBindings());
                $executionTime += $time;

                if ($result->rowCount() === 1) {
                    $return[] = $this->pdo->lastInsertId();
                }
            }
        }

        $this->fireEvents('after-insert', $return, $executionTime);

        return $return;
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function insert($data)
    {
        return $this->doInsert($data, 'insert');
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function insertIgnore($data)
    {
        return $this->doInsert($data, 'insertignore');
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function replace($data)
    {
        return $this->doInsert($data, 'replace');
    }

    /**
     * @param $data
     *
     * @return $this
     */
    public function update($data)
    {
        $eventResult = $this->fireEvents('before-update');
        if (!is_null($eventResult)) {
            return $eventResult;
        }

        $queryObject = $this->getQuery('update', $data);

        list($response, $executionTime) = $this->statement($queryObject->getSql(), $queryObject->getBindings());
        $this->fireEvents('after-update', $queryObject, $executionTime);

        return $response;
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function updateOrInsert($data)
    {
        if ($this->first()) {
            return $this->update($data);
        } else {
            return $this->insert($data);
        }
    }

    /**
     * @param $data
     *
     * @return $this
     */
    public function onDuplicateKeyUpdate($data)
    {
        $this->addStatement('onduplicate', $data);
        return $this;
    }

    /**
     *
     */
    public function delete()
    {
        $eventResult = $this->fireEvents('before-delete');
        if (!is_null($eventResult)) {
            return $eventResult;
        }

        $queryObject = $this->getQuery('delete');

        list($response, $executionTime) = $this->statement($queryObject->getSql(), $queryObject->getBindings());
        $this->fireEvents('after-delete', $queryObject, $executionTime);

        return $response;
    }

    /**
     * @param string|array $tables Single table or array of tables
     *
     * @return QueryBuilderHandler
     * @throws Exception
     */
    public function table($tables)
    {
        if (!is_array($tables)) {
            // because a single table is converted to an array anyways,
            // this makes sense.
            $tables = func_get_args();
        }

        $instance = new static($this->connection, $this->getFetchMode());
        if ($this->cacheHandler) {
            $instance->setCacheHandler($this->cacheHandler);
        }
        $tables = $this->addTablePrefix($tables, false);
        $instance->addStatement('tables', $tables);
        return $instance;
    }

    /**
     * @param $tables
     *
     * @return $this
     */
    public function from($tables)
    {
        if (!is_array($tables)) {
            $tables = func_get_args();
        }

        $tables = $this->addTablePrefix($tables, false);
        $this->addStatement('tables', $tables);
        return $this;
    }

    /**
     * @param $fields
     *
     * @return $this
     */
    public function select($fields)
    {
        if (!is_array($fields)) {
            $fields = func_get_args();
        }

        $fields = $this->addTablePrefix($fields);
        $this->addStatement('selects', $fields);
        return $this;
    }

    /**
     * @param string $sql
     * @param array $bindings
     * @return $this
     */
    public function selectRaw($sql, $bindings = [])
    {
        $fields = $this->addTablePrefix($this->raw($sql, $bindings));
        $this->addStatement('selects', $fields);
        return $this;
    }

    /**
     * @param $fields
     *
     * @return $this
     */
    public function selectDistinct($fields)
    {
        $this->select($fields);
        $this->addStatement('distinct', true);
        return $this;
    }

    /**
     * @param $field
     *
     * @return $this
     */
    public function groupBy($field)
    {
        $field = $this->addTablePrefix($field);
        $this->addStatement('groupBys', $field);
        return $this;
    }

    /**
     * @param        $fields
     * @param string $defaultDirection
     *
     * @return $this
     */
    public function orderBy($fields, $defaultDirection = 'ASC')
    {
        if (!is_array($fields)) {
            $fields = array($fields);
        }

        foreach ($fields as $key => $value) {
            $field = $key;
            $type = $value;
            if (is_int($key)) {
                $field = $value;
                $type = $defaultDirection;
            }
            if (!$field instanceof Raw) {
                $field = $this->addTablePrefix($field);
            }
            $this->statements['orderBys'][] = compact('field', 'type');
        }

        return $this;
    }

    /**
     * @param $limit
     *
     * @return $this
     */
    public function limit($limit)
    {
        $this->statements['limit'] = $limit;
        return $this;
    }

    /**
     * @param $offset
     *
     * @return $this
     */
    public function offset($offset)
    {
        $this->statements['offset'] = $offset;
        return $this;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $joiner
     *
     * @return $this
     */
    public function having($key, $operator, $value, $joiner = 'AND')
    {
        $key = $this->addTablePrefix($key);
        $this->statements['havings'][] = compact('key', 'operator', 'value', 'joiner');
        return $this;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     *
     * @return $this
     */
    public function orHaving($key, $operator, $value)
    {
        return $this->having($key, $operator, $value, 'OR');
    }

    /**
     * If $filtedMode is enabled, then only non-empty $values will be handled.
     * Example:
     *      $qb->where('some-field', '>', $_GET['param'], isset($_GET['param']))
     * 
     * @param string $key
     * @param string $operator
     * @param string $value
     * @param boolean $filterMode
     *
     * @return $this
     */
    public function where($key, $operator = null, $value = null, $filterMode = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        } elseif (func_num_args() == 4) {
            if (!(bool)$filterMode) {
                return $this;
            }
        }

        return $this->whereHandler($key, $operator, $value);
    }

    /**
     * @param string $sql
     * @param array $bindings
     * @return $this
     */
    public function whereRaw($sql, $bindings = [])
    {
        return $this->whereHandler($this->raw($sql, $bindings));
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function orWhere($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }

        return $this->whereHandler($key, $operator, $value, 'OR');
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function whereNot($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }
        return $this->whereHandler($key, $operator, $value, 'AND NOT');
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function orWhereNot($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }
        return $this->whereHandler($key, $operator, $value, 'OR NOT');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function whereIn($key, $values)
    {
        if ($values === []) {
            return $this->where($this->raw('1 = 2'));
        }
        return $this->whereHandler($key, 'IN', $values, 'AND');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function whereNotIn($key, $values)
    {
        if ($values === []) {
            return $this;
        }
        return $this->whereHandler($key, 'NOT IN', $values, 'AND');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function orWhereIn($key, $values)
    {
        return $this->whereHandler($key, 'IN', $values, 'OR');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function orWhereNotIn($key, $values)
    {
        return $this->whereHandler($key, 'NOT IN', $values, 'OR');
    }

    /**
     * @param $key
     * @param $valueFrom
     * @param $valueTo
     *
     * @return $this
     */
    public function whereBetween($key, $valueFrom, $valueTo)
    {
        return $this->whereHandler($key, 'BETWEEN', array($valueFrom, $valueTo), 'AND');
    }

    /**
     * @param $key
     * @param $valueFrom
     * @param $valueTo
     *
     * @return $this
     */
    public function orWhereBetween($key, $valueFrom, $valueTo)
    {
        return $this->whereHandler($key, 'BETWEEN', array($valueFrom, $valueTo), 'OR');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function whereNull($key)
    {
        return $this->whereNullHandler($key);
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function whereNotNull($key)
    {
        return $this->whereNullHandler($key, 'NOT');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function orWhereNull($key)
    {
        return $this->whereNullHandler($key, '', 'or');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function orWhereNotNull($key)
    {
        return $this->whereNullHandler($key, 'NOT', 'or');
    }

    protected function whereNullHandler($key, $prefix = '', $operator = '')
    {
        $key = $this->adapterInstance->wrapSanitizer($this->addTablePrefix($key));
        return $this->{$operator . 'Where'}($this->raw("{$key} IS {$prefix} NULL"));
    }

    /**
     * @param        $table
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $type
     *
     * @return $this
     */
    public function join($table, $key, $operator = null, $value = null, $type = 'inner')
    {
        if (!$key instanceof \Closure) {
            $key = function ($joinBuilder) use ($key, $operator, $value) {
                $joinBuilder->on($key, $operator, $value);
            };
        }

        // Build a new JoinBuilder class, keep it by reference so any changes made
        // in the closure should reflect here
        $joinBuilder = $this->container->build('\\Pixie\\QueryBuilder\\JoinBuilder', array($this->connection));
        $joinBuilder = &$joinBuilder;
        // Call the closure with our new joinBuilder object
        $key($joinBuilder);
        $table = $this->addTablePrefix($table, false);
        // Get the criteria only query from the joinBuilder object
        $this->statements['joins'][] = compact('type', 'table', 'joinBuilder');

        return $this;
    }

    /**
     * Runs a transaction
     *
     * @param callable $callback
     *
     * @return $this
     */
    public function transaction(callable $callback)
    {
        try {
            // Begin the PDO transaction
            $this->pdo->beginTransaction();

            // Get the Transaction class
            $transaction = $this->container->build('\\Pixie\\QueryBuilder\\Transaction', array($this->connection));

            // Call closure
            $callback($transaction);

            // If no errors have been thrown or the transaction wasn't completed within
            // the closure, commit the changes
            $this->pdo->commit();

            return $this;
        } catch (TransactionHaltException $e) {
            // Commit or rollback behavior has been handled in the closure, so exit
            return $this;
        } catch (\Exception $e) {
            // something happened, rollback changes
            $this->pdo->rollBack();
            return $this;
        }
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function leftJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'left');
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function rightJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'right');
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function innerJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'inner');
    }

    /**
     * Add a raw query
     *
     * @param $value
     * @param $bindings
     *
     * @return mixed
     */
    public function raw($value, $bindings = array())
    {
        return $this->container->build('\\Pixie\\QueryBuilder\\Raw', array($value, $bindings));
    }

    /**
     * Return PDO instance
     *
     * @return PDO
     */
    public function pdo()
    {
        return $this->pdo;
    }

    /**
     * @param Connection $connection
     *
     * @return $this
     */
    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * @return Connection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $joiner
     *
     * @return $this
     */
    protected function whereHandler($key, $operator = null, $value = null, $joiner = 'AND')
    {
        $key = $this->addTablePrefix($key);
        $this->statements['wheres'][] = compact('key', 'operator', 'value', 'joiner');
        return $this;
    }

    /**
     * Add table prefix (if given) on given string.
     *
     * @param      $values
     * @param bool $tableFieldMix If we have mixes of field and table names with a "."
     *
     * @return array|mixed
     */
    public function addTablePrefix($values, $tableFieldMix = true)
    {
        if (is_null($this->tablePrefix)) {
            return $values;
        }

        // $value will be an array and we will add prefix to all table names

        // If supplied value is not an array then make it one
        $single = false;
        if (!is_array($values)) {
            $values = array($values);
            // We had single value, so should return a single value
            $single = true;
        }

        $return = array();

        foreach ($values as $key => $value) {
            // It's a raw query, just add it to our return array and continue next
            if ($value instanceof Raw || $value instanceof \Closure) {
                $return[$key] = $value;
                continue;
            }

            // If key is not integer, it is likely a alias mapping,
            // so we need to change prefix target
            $target = &$value;
            if (!is_int($key)) {
                $target = &$key;
            }

            if (!$tableFieldMix || ($tableFieldMix && strpos($target, '.') !== false)) {
                $target = $this->tablePrefix . $target;
            }

            $return[$key] = $value;
        }

        // If we had single value then we should return a single value (end value of the array)
        return $single ? end($return) : $return;
    }

    /**
     * @param $key
     * @param $value
     */
    protected function addStatement($key, $value)
    {
        if (!is_array($value)) {
            $value = array($value);
        }

        if (!array_key_exists($key, $this->statements)) {
            $this->statements[$key] = $value;
        } else {
            $this->statements[$key] = array_merge($this->statements[$key], $value);
        }
    }

    /**
     * @param $event
     * @param $table
     *
     * @return callable|null
     */
    public function getEvent($event, $table = ':any')
    {
        return $this->connection->getEventHandler()->getEvent($event, $table);
    }

    /**
     * @param          $event
     * @param string $table
     * @param callable $action
     *
     * @return void
     */
    public function registerEvent($event, $table, \Closure $action)
    {
        $table = $table ?: ':any';

        if ($table != ':any') {
            $table = $this->addTablePrefix($table, false);
        }

        return $this->connection->getEventHandler()->registerEvent($event, $table, $action);
    }

    /**
     * @param          $event
     * @param string $table
     *
     * @return void
     */
    public function removeEvent($event, $table = ':any')
    {
        if ($table != ':any') {
            $table = $this->addTablePrefix($table, false);
        }

        return $this->connection->getEventHandler()->removeEvent($event, $table);
    }

    /**
     * @param      $event
     * @return mixed
     */
    public function fireEvents($event)
    {
        $params = func_get_args();
        array_unshift($params, $this);
        return call_user_func_array(array($this->connection->getEventHandler(), 'fireEvents'), $params);
    }

    /**
     * @return array
     */
    public function getStatements()
    {
        return $this->statements;
    }

    /**
     * @return int will return PDO Fetch mode
     */
    public function getFetchMode()
    {
        return !empty($this->fetchParameters) ?
            current($this->fetchParameters) : PDO::FETCH_OBJ;
    }
}
