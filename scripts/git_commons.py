import os
import logging
from git import Repo, InvalidGitRepositoryError

logger = logging.getLogger(__name__)


def get_git_repo(repo_dir: str) -> Repo:
    """
    Returns the git repository used for VCS of source and transformed data files. As well as load logs.
    If it does not exist, it will create one.

    :return: git.Repo
    """
    if not os.path.exists(repo_dir):
        return init_git_repo(repo_dir)
    try:
        repo = Repo(repo_dir)
        init_gitignore(repo)
        return repo
    except InvalidGitRepositoryError:
        return init_git_repo(repo_dir)


def init_gitignore(repo: Repo):
    gitignore = os.path.realpath(os.path.join(repo.working_tree_dir, '.gitignore'))
    if os.path.isfile(gitignore):
        return

    ignore_list = ['.done-*', '.DS_Store']
    logger.debug('Writing git ignore file')
    with open(gitignore, 'w') as f:
        f.write('\n'.join(ignore_list))
        f.write('\n')

    repo.index.add([gitignore])
    repo.index.commit('Initial commit.')


def init_git_repo(repo_dir) -> Repo:
    os.makedirs(repo_dir, exist_ok=True)
    logger.info('Initializing git repository: {}'.format(repo_dir))
    repo = Repo.init(os.path.realpath(repo_dir))
    init_gitignore(repo)
    return repo
