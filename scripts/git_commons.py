import os

import git


def get_git_repo(repo_dir):
    """
    Returns the git repository used for VCS of source and transformed data files. As well as load logs.
    If it does not exist, it will create one.

    :return: git.Repo
    """
    if not os.path.exists(repo_dir):
        return init_git_repo(repo_dir)

    try:
        return git.Repo(repo_dir)
    except git.InvalidGitRepositoryError:
        return init_git_repo(repo_dir)


def init_git_repo(repo_dir):
    os.makedirs(repo_dir, exist_ok=True)
    print('Initializing git repository: {}'.format(repo_dir))
    r = git.Repo.init(os.path.realpath(repo_dir))
    ignore_list = ['.done-*', '.DS_Store']

    gitignore = os.path.realpath(os.path.join(repo_dir, '.gitignore'))

    with open(gitignore, 'w') as f:
        f.write('\n'.join(ignore_list))

    r.index.add([gitignore])
    r.index.commit('Initial commit.')
    return r
