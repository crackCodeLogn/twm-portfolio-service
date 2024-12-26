package com.vv.personal.twm.portfolio.remote.feign;

import com.vv.personal.twm.artifactory.generated.bank.BankProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.ping.remote.feign.PingFeign;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author Vivek
 * @since 2024-08-11
 */
@FeignClient("twm-bank-crdb-service")
public interface BankCrdbServiceFeign extends PingFeign {

  @GetMapping("/crdb/bank/fixed-deposits?field={field}&value={value}")
  FixedDepositProto.FixedDepositList getFixedDeposits(
      @PathVariable("field")
          String field, // BANK, USER, ORIGINAL_USER, KEY, EMPTY - return all if EMPTY
      @PathVariable("value") String value);

  @GetMapping("/crdb/bank/bank-accounts?field={field}&value={value}")
  BankProto.BankAccounts getBankAccounts(
      @PathVariable("field") String field, @PathVariable("value") String value);
}
