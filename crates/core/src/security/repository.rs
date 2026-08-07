use crate::common::{RepositoryResult, Versioned, WriteOutcome};
use crate::issuer::IssuerId;
use crate::security::patch::SecurityPatch;
use crate::security::{Security, SecurityId};
use ftracker_identifiers::Isin;
use std::rc::Rc;
use std::sync::Arc;

#[cfg_attr(test, mockall::automock)]
pub trait SecurityRepository {
    fn find_by_id(&self, id: &SecurityId) -> RepositoryResult<Option<Versioned<Security>>>;

    fn find_by_isin(&self, isin: &Isin) -> RepositoryResult<Option<Versioned<Security>>>;

    fn list_all(&self) -> RepositoryResult<Vec<Versioned<Security>>>;

    fn list_by_issuer(&self, issuer_id: &IssuerId) -> RepositoryResult<Vec<Versioned<Security>>>;

    fn list_paged(
        &self,
        after: Option<SecurityId>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<Security>>>;

    fn exists(&self, id: &SecurityId) -> RepositoryResult<bool>;

    fn exists_by_isin(&self, isin: &Isin) -> RepositoryResult<bool>;

    fn insert(&self, security: &Security) -> RepositoryResult<()>;

    fn apply_patch(
        &self,
        id: &SecurityId,
        expected_version: u32,
        patch: SecurityPatch,
    ) -> RepositoryResult<WriteOutcome>;

    fn update(&self, security: &Security, expected_version: u32) -> RepositoryResult<WriteOutcome>;

    fn delete(&self, id: &SecurityId, expected_version: u32) -> RepositoryResult<WriteOutcome>;
}

macro_rules! delegate_security_repository {
    ($ty:ty) => {
        impl<R: SecurityRepository + ?Sized> SecurityRepository for $ty {
            fn find_by_id(&self, id: &SecurityId) -> RepositoryResult<Option<Versioned<Security>>> {
                (**self).find_by_id(id)
            }
            fn find_by_isin(&self, isin: &Isin) -> RepositoryResult<Option<Versioned<Security>>> {
                (**self).find_by_isin(isin)
            }
            fn list_all(&self) -> RepositoryResult<Vec<Versioned<Security>>> {
                (**self).list_all()
            }
            fn list_by_issuer(
                &self,
                issuer_id: &IssuerId,
            ) -> RepositoryResult<Vec<Versioned<Security>>> {
                (**self).list_by_issuer(issuer_id)
            }
            fn list_paged(
                &self,
                after: Option<SecurityId>,
                limit: u32,
            ) -> RepositoryResult<Vec<Versioned<Security>>> {
                (**self).list_paged(after, limit)
            }
            fn exists(&self, id: &SecurityId) -> RepositoryResult<bool> {
                (**self).exists(id)
            }
            fn exists_by_isin(&self, isin: &Isin) -> RepositoryResult<bool> {
                (**self).exists_by_isin(isin)
            }
            fn insert(&self, security: &Security) -> RepositoryResult<()> {
                (**self).insert(security)
            }
            fn apply_patch(
                &self,
                id: &SecurityId,
                expected_version: u32,
                patch: SecurityPatch,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).apply_patch(id, expected_version, patch)
            }
            fn update(
                &self,
                security: &Security,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).update(security, expected_version)
            }
            fn delete(
                &self,
                id: &SecurityId,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).delete(id, expected_version)
            }
        }
    };
}

delegate_security_repository!(Box<R>);
delegate_security_repository!(Rc<R>);
delegate_security_repository!(Arc<R>);
