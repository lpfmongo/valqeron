use crate::issuer::repository::IssuerRepository;
use crate::security::Security;
use crate::security::error::RegisterSecurityError;
use crate::security::repository::SecurityRepository;

/// Registers a new security after validating its cross-aggregate references:
/// the issuer must exist, the ISIN (when present) must be free, and a
/// depositary receipt's underlying security must already be registered.
pub fn register_security<S, I>(
    securities: &S,
    issuers: &I,
    security: &Security,
) -> Result<(), RegisterSecurityError>
where
    S: SecurityRepository + ?Sized,
    I: IssuerRepository + ?Sized,
{
    if !issuers.exists(security.issuer_id())? {
        return Err(RegisterSecurityError::UnknownIssuer);
    }

    if let Some(isin) = security.isin()
        && securities.exists_by_isin(isin)?
    {
        return Err(RegisterSecurityError::DuplicateIsin);
    }

    if let Some(underlying) = security.underlying_security_id()
        && !securities.exists(underlying)?
    {
        return Err(RegisterSecurityError::UnknownUnderlyingSecurity);
    }

    securities.insert(security)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::issuer::IssuerId;
    use crate::issuer::repository::MockIssuerRepository;
    use crate::security::repository::MockSecurityRepository;
    use crate::security::{SecurityId, SecurityKind};
    use ftracker_identifiers::Isin;

    const VALE_ON_ISIN: &str = "BRVALEACNOR0";

    fn common_share_with_isin() -> Option<Security> {
        let isin = Isin::new(VALE_ON_ISIN).ok()?;
        Security::builder(IssuerId::new(), SecurityKind::CommonShare)
            .isin(isin)
            .build()
            .ok()
    }

    fn adr_with_underlying(underlying: SecurityId) -> Option<Security> {
        Security::builder(IssuerId::new(), SecurityKind::DepositaryReceipt)
            .underlying_security_id(underlying)
            .build()
            .ok()
    }

    #[test]
    fn register_inserts_when_references_and_isin_are_valid() {
        let mut issuers = MockIssuerRepository::new();
        issuers.expect_exists().returning(|_| Ok(true));

        let mut securities = MockSecurityRepository::new();
        securities.expect_exists_by_isin().returning(|_| Ok(false));
        securities.expect_insert().returning(|_| Ok(()));

        let Some(security) = common_share_with_isin() else {
            return;
        };
        assert!(register_security(&securities, &issuers, &security).is_ok());
    }

    #[test]
    fn register_rejects_unknown_issuer_before_insert() {
        let mut issuers = MockIssuerRepository::new();
        issuers.expect_exists().returning(|_| Ok(false));

        let mut securities = MockSecurityRepository::new();
        // insert must never be called for an unknown issuer.
        securities.expect_insert().never();

        let Some(security) = common_share_with_isin() else {
            return;
        };
        assert!(matches!(
            register_security(&securities, &issuers, &security),
            Err(RegisterSecurityError::UnknownIssuer)
        ));
    }

    #[test]
    fn register_rejects_duplicate_isin_before_insert() {
        let mut issuers = MockIssuerRepository::new();
        issuers.expect_exists().returning(|_| Ok(true));

        let mut securities = MockSecurityRepository::new();
        securities.expect_exists_by_isin().returning(|_| Ok(true));
        // insert must never be called on a duplicate ISIN.
        securities.expect_insert().never();

        let Some(security) = common_share_with_isin() else {
            return;
        };
        assert!(matches!(
            register_security(&securities, &issuers, &security),
            Err(RegisterSecurityError::DuplicateIsin)
        ));
    }

    #[test]
    fn register_rejects_unknown_underlying_before_insert() {
        let mut issuers = MockIssuerRepository::new();
        issuers.expect_exists().returning(|_| Ok(true));

        let mut securities = MockSecurityRepository::new();
        securities.expect_exists().returning(|_| Ok(false));
        // insert must never be called when the underlying is missing.
        securities.expect_insert().never();

        let Some(adr) = adr_with_underlying(SecurityId::new()) else {
            return;
        };
        assert!(matches!(
            register_security(&securities, &issuers, &adr),
            Err(RegisterSecurityError::UnknownUnderlyingSecurity)
        ));
    }

    #[test]
    fn register_inserts_depositary_receipt_when_underlying_exists() {
        let mut issuers = MockIssuerRepository::new();
        issuers.expect_exists().returning(|_| Ok(true));

        let mut securities = MockSecurityRepository::new();
        securities.expect_exists().returning(|_| Ok(true));
        securities.expect_insert().returning(|_| Ok(()));

        let Some(adr) = adr_with_underlying(SecurityId::new()) else {
            return;
        };
        assert!(register_security(&securities, &issuers, &adr).is_ok());
    }
}
