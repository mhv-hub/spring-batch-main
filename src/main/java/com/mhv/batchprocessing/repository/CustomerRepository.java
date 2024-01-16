package com.mhv.batchprocessing.repository;

import com.mhv.batchprocessing.entity.TransformedCustomer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepository extends JpaRepository<TransformedCustomer, Long> {
}
