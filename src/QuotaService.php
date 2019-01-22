<?php
namespace PHPFacile\Booking\Quota\Db\Service;

use PHPFacile\Booking\Quota\Service\QuotaService as AbstractQuotaService;
use PHPFacile\Booking\Service\BookingServiceInterface;

use Zend\Db\Adapter\Adapter;
use Zend\Db\Adapter\AdapterInterface;
use Zend\Db\Sql\Expression;
use Zend\Db\Sql\Sql;
use Zend\Db\Sql\Where;

class QuotaService extends AbstractQuotaService
{
    /**
     * Database adapter
     *
     * @var AdapterInterface $adapter
     */
    protected $adapter;

    /**
     * Default database name
     *
     * @var string $fieldPoolId
     */
    protected $tableName = 'bookings';

    /**
     * Default database field name of storing pool id
     *
     * @var string $fieldPoolId
     */
    protected $fieldPoolId = 'pool_id';

    /**
     * Constructor
     *
     * @param AdapterInterface $adapter           Database adapter
     * @param array            $bookingMappingCfg Configuration to use custom database field names
     *
     * @return QuotaService
     */
    public function __construct(AdapterInterface $adapter, $bookingMappingCfg)
    {
        $this->adapter = $adapter;
        if (true === array_key_exists('bookings', $bookingMappingCfg)) {
            if (true === array_key_exists('resource', $bookingMappingCfg['bookings'])) {
                $this->tableName = $bookingMappingCfg['bookings']['resource'];
            }

            if (true === array_key_exists('fields', $bookingMappingCfg['bookings'])) {
                if (true === array_key_exists('pool_id', $bookingMappingCfg['bookings']['fields'])) {
                    $this->fieldPoolId = $bookingMappingCfg['bookings']['fields']['pool_id'];
                }
            }
        }
    }

    /**
     * Returns true if the quota is reached for a given pool of item
     *
     * @param Sql            $sql     Sql
     * @param Select         $query   Query
     * @param integer|string $poolId  Id of the pool (of item that can be booked)
     * @param mixed          $context Context so as to be able to manage different quotas depending on user profile for example (not yet taken into account)
     *
     * @return boolean
     */
    public function isQuotaReachedBasedOnCoreDbQuery($sql, $query, $poolId, $context)
    {
        $quota = $this->getPoolBasicQuota($poolId);
        if (null === $quota) {
            return false;
        }

        $stmt = $sql->prepareStatementForSqlObject($query);
        $rows = $stmt->execute();
        $row  = $rows->current();

        return ($row['c'] >= $quota);
    }

    /**
     * Returns true if the quota is reached for a given pool of item
     * taking into account a context (ex: maybe there is a quota of children for a given show)
     *
     * FIXME Might have to be able to return a user friendly error message
     * in case quota is reached (ex: you're not allowed to purchase more than 3 items)
     *
     * REM: Called before pre-reservation
     *
     * @param integer|string $poolId  Id of the pool (of item that can be booked)
     * @param mixed          $context Context so as to be able to manage different quotas depending on user profile for example
     *
     * @return boolean True if the quota is reached
     */
    public function isQuotaReached($poolId, $context = null)
    {
        // FIXME Quota may depend on many other attributes
        // Here we only consider a quota based on nb of units booked
        $sql   = new Sql($this->adapter);
        $where = new Where();
        $where->equalTo($this->fieldPoolId, $poolId);
        // REM we have to consider BookingServiceInterface::BOOKING_STATUS_PREBOOKING_ABOUT_TO_BE_CANCELLED
        // as part of the quota because otherwise we might have
        // problems with quota if the user want to re-reserve this ItemInstance
        // Hummm.... not so sure
        $where->in(
            'status',
            [
                BookingServiceInterface::BOOKING_STATUS_PREBOOKED,
                BookingServiceInterface::BOOKING_STATUS_BOOKED,
                // BookingServiceInterface::ITEM_STATUS_ABOUT_TO_BE_SET_AVAILABLE,
            ]
        );
        $query = $sql
            ->select($this->tableName)
            ->columns(['c' => new Expression('COUNT(*)')])
            ->where($where);

        return $this->isQuotaReachedBasedOnCoreDbQuery($sql, $query, $poolId, $context);
    }

    /**
     * Returns true if validating a given booking set (several bookings) would
     * lead to a quota limit overpassed
     *
     * FIXME Might have to be able to return a user friendly error message
     * in case quota is reached (ex: you're not allowed to purchase more than 3 items)
     *
     * REM: Called before pre-reservation
     *
     * @param integer|string $poolId       Id of the pool (of item that can be booked)
     * @param integer|string $bookingSetId Id of the booking set
     * @param mixed          $context      Context so as to be able to manage different quotas depending on user profile for example
     *
     * @return boolean True if the quota is reached
     */
    public function isOverQuota($poolId, $bookingSetId, $context = null)
    {
        // FIXME Quota may depend on many other attributes Cf. $context
        //
        // How many booking are there (either completed or in progress)
        // excluding those in progress if they started after the current one ?
        // By the way... When did this current booking session started ?
        // TODO Allow passing the 2 result values in parameters so
        // as to avoid an extra database call (as they might be already known)???
        $sql   = new Sql($this->adapter);
        $where = new Where();
        $where->equalTo($this->fieldPoolId, $poolId);
        $where->equalTo('booking_set_id', $bookingSetId);
        $query = $sql
            ->select($this->tableName)
            ->where($where)
            // TAKE_CARE "id DESC" assumes id are sequential
            ->order('id DESC')
            ->limit(1);
        $stmt = $sql->prepareStatementForSqlObject($query);
        $rows = $stmt->execute();
        $row  = $rows->current();
        if (false === $row) {
            throw new \Exception('Oups... booking set not found');
        }

        $bookingSetPreBookingStartDateTimeUTCStr = $row['status_datetime_utc'];
        $bookingSetMaxId = $row['id'];

        // REM we have to consider BookingServiceInterface::BOOKING_STATUS_PREBOOKING_ABOUT_TO_BE_CANCELLED
        // as part of the quota because otherwise we might have
        // problems with quota if the (other) user want to re-book a unit in this pool
        // Hummm.... not so sure
        $where = new Where();
        $where->equalTo($this->fieldPoolId, $poolId);
        $where
            ->nest
                ->in('status', array(BookingServiceInterface::BOOKING_STATUS_BOOKED,
                                  BookingServiceInterface::BOOKING_STATUS_PREBOOKING_ABOUT_TO_BE_CANCELLED))
                ->or
                ->nest
                    ->in('status', array(BookingServiceInterface::BOOKING_STATUS_PREBOOKED))
                    ->nest
                        ->lessThan('status_datetime_utc', $bookingSetPreBookingStartDateTimeUTCStr)
                        ->or
                        ->nest
                            // To avoid pb with several bookings done at the same datetime
                            // we're going to check the id
                            ->equalTo('status_datetime_utc', $bookingSetPreBookingStartDateTimeUTCStr)
                            ->lessThanOrEqualTo('id', $bookingSetMaxId) // TAKECARE Assumes id are sequential
                        ->unnest
                    ->unnest
                ->unnest
            ->unnest;

        $query = $sql
            ->select($this->tableName)
            ->columns(['c' => new Expression('COUNT(*)')])
            ->where($where);

        return $this->isQuotaReachedBasedOnCoreDbQuery($sql, $query, $poolId, $context);
    }

}
